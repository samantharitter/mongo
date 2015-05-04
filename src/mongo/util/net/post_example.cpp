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
#include <queue>

#include "asio.hpp"

#include "mongo/unittest/unittest.h"

namespace mongo {
    namespace net {

        class DatabaseOperationRunner {
        public:

            DatabaseOperationRunner() : _service() {}

            void startup() {
                // call run() in a separate thread
                _serviceRunner = std::thread([this] {
                        // enqueue some work so io_service doesn't quit
                        asio::io_service::work work(_service);
                        std::cout << "DB: running service\n" << std::flush;
                        _service.run();
                        std::cout << "DB: done running service\n" << std::flush;
                    });
            }

            void runCommand(std::string cmd, int id) {
                std::cout << "DB: running a command: " << cmd << "\n" << std::flush;
                std::cout << "DB: This might take a while. Please, have a seat.\n" << std::flush;
                sleep(5);
                std::cout << "DB: all done!\n" << std::flush;

                // tell network layer that connection 'id' is unblocked
            }

            void shutdown() {
                _service.stop();
                _serviceRunner.join();
            }

            asio::io_service _service;
            std::thread _serviceRunner;
        };

        class PostExample : public mongo::unittest::Test {
        public:

            PostExample() :
                _messages(),
                _db(),
                _connectionCount(0),
                _service(),
                _timer(_service)
            {}

            // Basic network connection state machine:
            //
            // state 1: receive message
            // state 2: send message to db
            // state 3: wait for db operation to complete
            //
            // repeat

            // STATE 0:
            // kick off the state machine
            // pretend this was called by do_accept, and given a socket
            void newConnection(int sock, int id) {
                std::cout << "TEST: new connection accepted\n" << std::flush;
                // post() this to our service object so the
                // state machine begins asynchronously
                asio::post(_service,
                           [this, sock, id]() {
                               std::cout << "TEST: opened connection " << id <<"\n" << std::flush;
                               receiveMessage(sock, id);
                           });
            }

            // STATE 1:
            // "receive a message"
            void receiveMessage(int sock, int id) {
                std::cout << "TEST: attempting to get message for connection "
                          << id << "\n" << std::flush;
                // pretend this was in handler for async_read on socket
                // instead of in a call to post()
                asio::post(_service,
                           [this, sock, id]() {
                               if (_messages.empty()) {
                                   std::cout << "TEST: connection " << id
                                             << "closed\n" << std::flush;
                                   return;
                               }
                               std::cout << "TEST: received message on connection "
                                         << id << "\n" << std::flush;
                               std::string message = _messages.front();
                               _messages.pop();
                               sendMessageToDatabase(message, sock, id);
                           });
            }

            // STATE 2:
            // send message to database to run
            void sendMessageToDatabase(std::string message, int sock, int id) {
                // post() to the db's service object so
                // cmd is run by the db's worker thread pool
                // (in this case a single thread w/e)
                std::cout << "TEST: running command for connection " << id << ":\n"
                          << "\t" << message << "\n" << std::flush;
                asio::post(_db._service,
                           [this, message, sock, id]() {
                               // this is a blocking call, while it executes
                               // in _db our service object can do other things.
                               _db.runCommand(message, id);
                               // after running cmd, post() back to our service
                               // object to keep running state machine
                               asio::post(_service,
                                          [this, sock, id]() {
                                              cmdFinished(sock, id);
                                          });
                           });
            }

            // STATE 3:
            // command has been completed by database
            void cmdFinished(int sock, int id) {
                std::cout << "TEST: command finished for connection " << id << "\n" << std::flush;
                asio::post(_service,
                           [this, sock, id]() {
                               receiveMessage(sock, id);
                           });
            }

            void doStuff() {
                std::cout << "TEST: doing some other stuff...\n" << std::flush;
                // use a timer so we don't spam ourselves
                _timer.expires_from_now(std::chrono::seconds(1));
                _timer.async_wait([this](std::error_code ec) {
                        doStuff();
                    });
            }

            void startup() {
                //do_accept();

                // call run() in a separate thread
                _serviceRunner = std::thread([this] {
                        // normally we'd call do_accept() to power service
                        asio::io_service::work work(_service);
                        std::cout << "TEST: running service\n" << std::flush;
                        _service.run();
                        std::cout << "TEST: done running service\n" << std::flush;
                    });

                // start the "database"
                _db.startup();
            }

            void shutdown() {
                _service.stop();
                _serviceRunner.join();

                _db.shutdown();
            }

            std::queue<std::string> _messages;

            DatabaseOperationRunner _db;

            int _connectionCount;
            asio::io_service _service;
            asio::steady_timer _timer;

            std::thread _serviceRunner;
        };

        TEST_F(PostExample, Example) {
            startup();

            // enqueue some messages
            _messages.push("find({ a:1 })");
            _messages.push("update({ $inc : { $a : 1 }})");
            _messages.push("removeOne({ a:1 })");

            // pretend we actually opened a connection and sent a message
            newConnection(0, 1);

            // simulate "other stuff" that service can do while db runs cmd
            doStuff();

            do {
                sleep(1);
            } while (!_messages.empty());

            shutdown();
        }

    } // namespace net
} // namespace mongo
