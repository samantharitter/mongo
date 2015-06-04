/*    Copyright 2015 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include <boost/make_shared.hpp>

#include "mongo/db/instance.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/util/net/asio_message_server.h"

namespace mongo {

    void ASIOMessageServer::_networkError(ClientConnection conn, std::error_code ec) {
        std::cout << "ASIOMessageServer: a network error occurred: " << ec << "\n\tclosing this connection\n";
    }

    void ASIOMessageServer::_runGetMore(ClientConnection conn, DbResponse dbresponse) {
        std::cout << "ASIOMessageServer: running getmore\n";
        verify(dbresponse.exhaustNS.size() > 0);

        MsgData::View header = dbresponse.response->header();
        QueryResult::View qr = header.view2ptr();
        long long cursorid = qr.getCursorId();
        if( cursorid ) {
            verify( dbresponse.exhaustNS.size() && dbresponse.exhaustNS[0] );
            std::string ns = dbresponse.exhaustNS; // before reset() free's it...
            BufBuilder b(512);
            b.appendNum((int) 0 /* size set later in appendData() */);
            b.appendNum(header.getId());
            b.appendNum(header.getResponseTo());
            b.appendNum((int) dbGetMore);
            b.appendNum((int) 0);
            b.appendStr(ns);
            b.appendNum((int) 0); // ntoreturn
            b.appendNum(cursorid);

            // note: does this data fall off edge of world?
            conn->toRecv.reset();
            conn->toRecv.appendData(b.buf(), b.len());

            b.decouple();

            // back to STATE 3
            _process(conn);
        } else {
            // done, to step 1
            _handleIncomingMessage(conn);
        }
    }

    void ASIOMessageServer::_sendDatabaseResponse(ClientConnection conn, DbResponse dbresponse) {
        std::cout << "ASIOMessageServer: sending response to db\n";

        // assuming that all messages are simple...
        invariant(dbresponse.response->_buf != 0);

        asio::const_buffer buf(dbresponse.response->_buf, dbresponse.response->size());
        asio::async_write(*(conn->sock()), asio::buffer(buf),
                          [this, conn, dbresponse](std::error_code ec, std::size_t bytes) {
                              if (ec) {
                                  _networkError(conn, ec);
                              } else {
                                  if (dbresponse.exhaustNS.size() <= 0) {
                                      // done, to step 1
                                      _handleIncomingMessage(conn);
                                      return;
                                  }
                                  // continue to getmores
                                  _runGetMore(conn, dbresponse);
                              }
                          });
    }

    void ASIOMessageServer::_processAsync(ClientConnection conn) {
        // todo
    }

    void ASIOMessageServer::_processSync(ClientConnection conn) {
        std::cout << "ASIOMessageServer: processing message\n";

        // TODO: post this to db worker io service
        _service.post([this, conn]() {
                // init client object for this thread, then remove
                OperationContextImpl txn;
                DbResponse dbresponse;
                assembleResponse(&txn, conn->toRecv, dbresponse, conn->remote());
                if (!dbresponse.response) {
                    // to step 1
                    _handleIncomingMessage(conn);
                }
                else {
                    // to step 4
                    _sendDatabaseResponse(conn, dbresponse);
                }
            });
    }

    void ASIOMessageServer::_process(ClientConnection conn) {
        // todo switch intelligently
        _processSync(conn);
    }

    void ASIOMessageServer::_recvMessageBody(ClientConnection conn) {
        std::cout << "ASIOMessageServer: receiving message body...\n";

        // len = whole message length, data + header
        int len = conn->header.constView().getMessageLength();

        // todo: server code uses crazy padding hack, investigate.
        int z = (len+1023)&0xfffffc00;
        verify(z>=len);
        conn->md = reinterpret_cast<char *>(mongoMalloc(z));
        MsgData::View md_view = conn->md;

        // copy header data into master buffer
        int headerLen = sizeof(MSGHEADER::Value);
        memcpy(md_view.view2ptr(), &conn->header, headerLen);
        int bodyLength = len - headerLen;

        // todo: header validation

        // receive remaining data into md->data
        asio::async_read(*(conn->sock()), asio::buffer(md_view.data(), bodyLength),
                         [this, conn, md_view](asio::error_code ec, size_t bytes) {
                             if (ec) {
                                 _networkError(conn, ec);
                             } else {
                                 conn->toRecv.setData((char *)md_view.view2ptr(), true);
                                 // here, send to next state in state machine
                                 _process(conn);
                             }
                         });
    }

    void ASIOMessageServer::_recvMessageHeader(ClientConnection conn) {
        asio::async_read(*(conn->sock()),
                         asio::buffer(reinterpret_cast<char *>(&conn->header), sizeof(MSGHEADER::Value)),
                         [this, conn](asio::error_code ec, size_t bytes) {
                             if (ec) {
                                 _networkError(conn, ec);
                             } else {
                                 _recvMessageBody(conn);
                             }
                         });
    }

    void ASIOMessageServer::_handleIncomingMessage(ClientConnection conn) {
        std::cout << "ASIOMessageServer: handleIncomingMessage()\n";
        _recvMessageHeader(conn);
    }

    void ASIOMessageServer::_doAccept() {
        if (_shutdown) {
            return;
        }

        StickySocket sock(boost::make_shared<tcp::socket>(_service));
        _acceptor.async_accept(*sock,
                               [this, sock](std::error_code ec) {
                                   if (ec) {
                                       std::cout << "ASIOMessageServer accept error " << ec << "\n";
                                   } else {
                                       // need to create a client object
                                       std::cout << "ASIOMessageServer: new accepted connection\n";
                                       ClientConnection conn = boost::make_shared<Connection>(sock);
                                       _handleIncomingMessage(conn);
                                   }
                                   _doAccept();
                               });
    }

    void ASIOMessageServer::run() {
        std::cout << "ASIOMessageServer: run()";

        // set up our listening socket
        // TODO: add support for iplists
        tcp::resolver resolver(_service);
        tcp::endpoint endpoint = *resolver.resolve({"localhost", std::to_string(_port)});

        // Open the acceptor with the option to reuse the address (i.e. SO_REUSEADDR).
        _acceptor.open(endpoint.protocol());
        _acceptor.set_option(tcp::acceptor::reuse_address(true));
        _acceptor.bind(endpoint);
        _acceptor.listen();

        _doAccept();

        _serviceRunner = std::thread([this]() {
            std::cout << "ASIOMessageServer: launching io service runner\n";
            _service.run();
            std::cout << "ASIOMessageServer: io service runner returning\n";
        });

        // todo: rewrite server so we don't need to do this...
        // implement some better way of killing io service
        while (true) {
            sleep(100);
        }
    }

} // namespace mongo
