/**
 *    Copyright (C) 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include <thread>
#include <chrono>

#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

#include "mongo/db/dbmessage.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/repl/network_interface_asio.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message.h"

namespace mongo {
    namespace repl {

        // need some kind of class to sit and receive messages
        // and need a way to assert that messages on this class are received
        // also, this class should set up the network interface and provide utility messages
        class ReplTestASIO : public mongo::unittest::Test {
        public:
            ReplTestASIO() : _messageCount(0),
                             _shutdown(false),
                             _service(),
                             _acceptor(_service),
                             _net(boost::make_shared<NetworkInterfaceASIO>()) {
                std::cout << "TEST: hum de dum test constructor\n" << std::flush;
            }

            // todo add dtor that calls shutdown on interface

            void init() {
                std::cout << "TEST: init()\n" << std::flush;
                _net->startup();
            }

            void do_accept() {
                std::cout << "TEST: beginning do_accept()\n" << std::flush;

                // TODO there should be a way of doing this from within IO service
                if (_shutdown) {
                    return;
                }

                boost::shared_ptr<tcp::socket> sock(boost::make_shared<tcp::socket>(_service));
                _acceptor.async_accept(*sock,
                                       [this, sock](std::error_code ec) {
                                           if (ec) {
                                               std::cout << "TEST: accept error\n" << std::flush;
                                           } else {
                                               std::cout << "TEST: handling new accepted connection\n" << std::flush;
                                               handleIncomingMsg(sock);
                                           }
                                           do_accept();
                    });
            }

            void process(Message& m) {
                // this is here as an example of where we might interface
                // with the current message-handling framework in mongod
                // after some logic this method would hand off to assembleResponse()
                MsgData::ConstView header(m.header().view2ptr());
                std::cout << "TEST: received a message: " << m.toString() << "\n";

                // get command out of message
                int op = m.operation();
                if (op == dbQuery) {
                    DbMessage d(m);
                    QueryMessage q(d);

                    // if is command:
                    const NamespaceString nsString(d.getns());
                    if (nsString.isCommand()) {
                        std::cout << "command " << q.query << " on " << q.ns << "\n";
                    } else {
                        std::cout << "query " << q.query << " on " << q.ns << "\n";
                    }
                } else if (op == dbGetMore) {
                    DbMessage d(m);
                    std::cout << "getmore on " << d.getns() << " for cursorid " << d.pullInt64() << "\n";
                } else {
                    std::cout << "something else\n";
                }
                std::cout << "TEST: done processing message\n" << std::flush;
            }

            void recvMsgBody(boost::shared_ptr<tcp::socket> sock,
                             boost::shared_ptr<MSGHEADER::Value> header,
                             int headerLen) {
                std::cout << "TEST: recvMsgBody()\n";

                // todo: error checking on len
                // len = whole message length, data + header
                int len = header->constView().getMessageLength();

                // todo: server code uses crazy padding hack, investigate.
                boost::shared_ptr<char[]> buf(new char[len]);
                boost::shared_ptr<MsgData::View> md(boost::make_shared<MsgData::View>(buf.get()));

                // copy header data into master buffer
                memcpy(md->view2ptr(), header.get(), headerLen);
                int bodyLength = len - headerLen;

                std::cout << "receiving message\n" << std::flush;

                // receive remaining data into md->data
                asio::async_read(*sock, asio::buffer(md->data(), bodyLength),
                                 [this, md, buf](asio::error_code ec, size_t bytes) {
                                     if (ec) {
                                         std::cout << "TEST: error receiving message body\n" << std::flush;
                                     } else {
                                         std::cout << "TEST: received message body\n" << std::flush;
                                         Message m;
                                         m.setData(md->view2ptr(), true);
                                         process(m);
                                     }
                                 });
            }

            void handleIncomingMsg(boost::shared_ptr<tcp::socket> sock) {
                std::cout << "TEST: handleIncomingMsg()\n";

                boost::shared_ptr<MSGHEADER::Value> header(boost::make_shared<MSGHEADER::Value>());
                int headerLen = sizeof(MSGHEADER::Value);

                asio::async_read(*sock, asio::buffer((char *)(header.get()), headerLen),
                    [this, sock, header, headerLen](asio::error_code ec, size_t bytes) {
                    if (ec) {
                        std::cout << "TEST: error receiving header\n" << std::flush;
                    } else {
                        std::cout << "TEST: received message header\n" << std::flush;
                        recvMsgBody(sock, header, headerLen);
                    }
                });
            }

            void startServer(int port) {
                std::cout << "TEST: launching thread to listen on port " << port << "\n" << std::flush;
                tcp::resolver resolver(_service);
                tcp::endpoint endpoint = *resolver.resolve({"localhost", std::to_string(port)});

                // Open the acceptor with the option to reuse the address (i.e. SO_REUSEADDR).
                _acceptor.open(endpoint.protocol());
                _acceptor.set_option(tcp::acceptor::reuse_address(true));
                _acceptor.bind(endpoint);
                _acceptor.listen();

                do_accept();

                // run io service
                _serviceRunner = std::thread([this]() {
                        std::cout << "TEST: run() starting\n" << std::flush;
                        _service.run();
                        std::cout << "TEST: run() has returned\n" << std::flush;
                        _acceptor.close();
                    });
                std::cout << "TEST: done launching server\n" << std::flush;
            }

            void stopServer() {
                std::cout << "TEST: shutting down test server\n" << std::flush;
                _shutdown = true;

                _service.stop();
                _serviceRunner.join();
            }

            boost::shared_ptr<NetworkInterfaceASIO> getNet() {
                return _net;
            }

            void waitForMessageCount(int count) {
                // there are more graceful ways to do this
                std::cout << "TEST: waiting for messages...\n" << std::flush;
                while (_messageCount < count) {
                    std::this_thread::sleep_for(std::chrono::milliseconds(100));
                }
                std::cout << "TEST: got messages, returning\n" << std::flush;
            }

            void receiveMessage(const ReplicationExecutor::ResponseStatus status) {
                std::cout << "TEST: request completion handler called\n" << std::flush;
                _messageCount++;
            }

        private:
            int _messageCount;
            bool _shutdown;
            asio::io_service _service;
            tcp::acceptor _acceptor;
            boost::shared_ptr<NetworkInterfaceASIO> _net;
            std::thread _serviceRunner;
        };

        TEST_F(ReplTestASIO, DummyTest) {
            std::cout << "TEST: beginning test\n" << std::flush;

            int runs = 1;
            int port = 12345;
            init();

            std::cout << "TEST: starting server\n" << std::flush;

            startServer(port);

            boost::shared_ptr<NetworkInterfaceASIO> net = getNet();
            const ReplicationExecutor::RemoteCommandRequest request(HostAndPort("localhost", port),
                                                                    "somedb",
                                                                    BSON( "a" << GTE << 0 ));

            for (int i = 0; i < runs; i++) {
                std::cout << "TEST: enqueuing work\n" << std::flush;
                net->startCommand(ReplicationExecutor::CallbackHandle(),
                                  request,
                                  stdx::bind(&ReplTestASIO::receiveMessage, this, ResponseStatus(Status::OK())));
            }

            waitForMessageCount(runs);
            std::cout << "TEST: shutting down\n" << std::flush;
            net->shutdown();
            stopServer();
        }

    } // namespace repl
} // namespace mongo
