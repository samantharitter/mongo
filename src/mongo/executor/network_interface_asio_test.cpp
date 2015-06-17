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

#include <chrono>
#include <memory>
#include <thread>

#include "mongo/db/dbmessage.h"
#include "mongo/db/namespace_string.h"
#include "mongo/executor/network_interface_asio.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message.h"

namespace mongo {
namespace executor {

    using asio::ip::tcp;

    // need some kind of class to sit and receive messages
    // and need a way to assert that messages on this class are received
    // also, this class should set up the network interface and provide utility messages
    class ExecTestASIO : public mongo::unittest::Test {
    public:
        ExecTestASIO() : _messageCount(0),
                         _shutdown(false),
                         _service(),
                         _acceptor(_service),
                         _net(stdx::make_unique<NetworkInterfaceASIO>())
        {}

        void init() {
            _net->startup();
        }

        void do_accept() {
            // TODO there should be a way of doing this from within IO service
            if (_shutdown) {
                return;
            }

            std::shared_ptr<tcp::socket> sock(std::make_shared<tcp::socket>(_service));
            _acceptor.async_accept(*sock,
                                   [this, sock](std::error_code ec) {
                                       if (ec) {
                                           LOG(3) << "TEST: accept error\n" << std::flush;
                                       } else {
                                           handleIncomingMsg(sock);
                                       }
                                       do_accept();
                                   });
        }

        void process(Message& m) {
            MsgData::ConstView header(m.header().view2ptr());

            // get command out of message
            int op = m.operation();
            if (op == dbQuery) {
                DbMessage d(m);
                QueryMessage q(d);

                // if is command:
                const NamespaceString nsString(d.getns());
                if (nsString.isCommand()) {
                    LOG(3) << "Received command " << q.query << " on " << q.ns << "\n";
                } else {
                    LOG(3) << "Received query " << q.query << " on " << q.ns << "\n";
                }
            } else if (op == dbGetMore) {
                DbMessage d(m);
                LOG(3) << "Received getmore on " << d.getns() << ", cursorid " << d.pullInt64() << "\n";
            } else {
                LOG(3) << "Received an unknown type of message\n";
            }
        }

        void recvMsgBody(std::shared_ptr<tcp::socket> sock,
                         std::shared_ptr<MSGHEADER::Value> header,
                         int headerLen) {
            // todo: error checking on len
            // len = whole message length, data + header
            int len = header->constView().getMessageLength();

            int z = (len+1023)&0xfffffc00;
            verify(z>=len);
            // todo, need a guard?
            std::shared_ptr<MsgData::View> md(std::make_shared<MsgData::View>(reinterpret_cast<char *>(mongoMalloc(z))));

            // copy header data into master buffer
            memcpy(md->view2ptr(), header.get(), headerLen);
            int bodyLength = len - headerLen;

            // receive remaining data into md->data
            asio::async_read(*sock, asio::buffer(md->data(), bodyLength),
                             [this, md](asio::error_code ec, size_t bytes) {
                                 if (ec) {
                                     LOG(3) << "TEST: error receiving message body\n" << std::flush;
                                 } else {
                                     Message m;
                                     m.setData(md->view2ptr(), true);
                                     process(m);
                                 }
                             });
        }

        void handleIncomingMsg(std::shared_ptr<tcp::socket> sock) {
            std::shared_ptr<MSGHEADER::Value> header(std::make_shared<MSGHEADER::Value>());
            int headerLen = sizeof(MSGHEADER::Value);

            asio::async_read(*sock, asio::buffer((char *)(header.get()), headerLen),
                             [this, sock, header, headerLen](asio::error_code ec, size_t bytes) {
                                 if (ec) {
                                     LOG(3) << "TEST: error receiving header\n" << std::flush;
                                 } else {
                                     recvMsgBody(sock, header, headerLen);
                                 }
                             });
        }

        void startServer(int port) {
            LOG(3) << "TEST: launching thread to listen on port " << port << "\n" << std::flush;
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
                    _service.run();
                    _acceptor.close();
                });
        }

        void stopServer() {
            _shutdown = true;
            _service.stop();
            _serviceRunner.join();
        }

        NetworkInterfaceASIO* getNet() {
            return _net.get();
        }

        void waitForMessageCount(int count) {
            while (_messageCount < count) {
                std::this_thread::sleep_for(std::chrono::milliseconds(100));
            }
        }

        void receiveMessage(const TaskExecutor::ResponseStatus status) {
            _messageCount++;
        }

    private:
        int _messageCount;
        bool _shutdown;
        asio::io_service _service;
        tcp::acceptor _acceptor;
        std::unique_ptr<NetworkInterfaceASIO> _net;
        std::thread _serviceRunner;
    };

    TEST_F(ExecTestASIO, DummyTest) {
        int runs = 1;
        int port = 12345;
        init();

        startServer(port);

        NetworkInterfaceASIO* net = getNet();
        const RemoteCommandRequest request(HostAndPort("localhost", port),
                                           "somedb",
                                           BSON( "a" << GTE << 0 ));

        for (int i = 0; i < runs; i++) {
            TaskExecutor::ResponseStatus status(NetworkInterface::Response(BSONObj(), Milliseconds(10)));
            net->startCommand(TaskExecutor::CallbackHandle(),
                              request,
                              stdx::bind(&ExecTestASIO::receiveMessage, this, status));
        }

        waitForMessageCount(runs);
        net->shutdown();
        stopServer();
    }

} // namespace repl
} // namespace mongo
