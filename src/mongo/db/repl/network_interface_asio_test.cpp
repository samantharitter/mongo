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

#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

#include "mongo/db/repl/network_interface_asio.h"
#include "mongo/unittest/unittest.h"
#include "mongo/util/log.h"

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

            void after_accept(boost::shared_ptr<tcp::socket> sock) {
                std::cout << "TEST: in after_accept()\n";
                char c[256];
                for(int i = 0; i < 256; i++) {
                    c[i] = '\0';
                }
                auto buf(asio::buffer(c, 256));
                size_t res;

                try {
                    do {
                        res = sock->read_some(buf);
                        std::cout << "TEST: received " << res << " bytes\n";
                    } while (res > 0);
                } catch(const std::exception& e) {
                    std::cout << "TEST: caught an exception in read: " << e.what() << "\n";
                }

                std::string s(c);
                std::cout << "TEST: received message " << s << "\n" << std::flush;
                //parseMessage(buf, res);

                for(int i = 0; i < 256; i++) {
                    std::cout << c[i];
                }
                std::cout << "\n" << std::flush;

                sock->close();
            }

            void do_accept() {
                std::cout << "TEST: beginning do_accept()\n" << std::flush;

                boost::shared_ptr<tcp::socket> sock(boost::make_shared<tcp::socket>(_service));
                _acceptor.async_accept(*sock,
                                       [this, sock](std::error_code ec) {
                                           if (ec) {
                                               std::cout << "TEST: accept error\n";
                                           } else {
                                               after_accept(sock);
                                           }
                                           do_accept();
                    });
            }

            // TODO: async method to recv MSGHEADER, completion handler calls method to
            // recv MSG body.

            void parseMessage(asio::mutable_buffer buf, size_t bytes) {
                // assumptions:
                // - @buf contains an entire message
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
                _serviceRunner = boost::thread([this]() {
                        std::cout << "TEST: run() starting\n" << std::flush;
                        _service.run();
                        std::cout << "TEST: run() has returned\n" << std::flush;
                    });
                std::cout << "TEST: done launching server\n" << std::flush;
            }

            void stopServer() {
                std::cout << "TEST: shutting down test server\n" << std::flush;
                _shutdown = true;
                _acceptor.close();
                _listener->boost::thread::join();

                _service.stop();
                _serviceRunner.join();
            }

            boost::shared_ptr<NetworkInterfaceASIO> getNet() {
                return _net;
            }

            void waitForMessageCount(int count) {
                // there are more graceful ways to do this
                while (_messageCount < count) {
                    std::cout << "TEST: waiting for messages...\n" << std::flush;
                    sleep(100);
                }
            }

            void receiveMessage(const ReplicationExecutor::ResponseStatus status) {
                std::cout << "TEST: request completion handler called\n" << std::flush;
                _messageCount++;
            }

        private:
            int _messageCount;
            bool _shutdown;
            boost::shared_ptr<boost::thread> _listener;
            asio::io_service _service;
            tcp::acceptor _acceptor;
            boost::shared_ptr<NetworkInterfaceASIO> _net;
            boost::thread _serviceRunner;
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
                                                                    BSON("hello" << "world"));
            sleep(3); // test needs a delay??

            for (int i = 0; i < runs; i++) {
                std::cout << "TEST: enqueuing work\n" << std::flush;
                net->startCommand(ReplicationExecutor::CallbackHandle(),
                                  request,
                                  stdx::bind(&ReplTestASIO::receiveMessage, this, ResponseStatus(Status::OK())));
            }

            waitForMessageCount(runs);
            net->shutdown();
            stopServer();
        }

    } // namespace repl
} // namespace mongo
