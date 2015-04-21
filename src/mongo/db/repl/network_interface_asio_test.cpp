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
            ReplTestASIO() : _messageCount(0) {
                _net = std::unique_ptr<NetworkInterfaceASIO>(new NetworkInterfaceASIO);
            }

            // todo add dtor that calls shutdown on interface

            void init() {
                std::cout << "init()\n";
                _net->startup();
            }

            void startServer(int port) {
                std::cout << "starting server on port " << port << "\n";
            }

            NetworkInterfaceASIO* getNet() {
                return _net.get();
            }

            void waitForMessageCount(int count) {
                // there are more graceful ways to do this
                while (_messageCount < count) {
                    std::cout << "waiting for messages...\n";
                    sleep(1);
                }
            }

            void receiveMessage(const ReplicationExecutor::ResponseStatus status) {
                std::cout << "callback called\n";
                _messageCount++;
            }

            int getMessageCount() {
                return _messageCount;
            }

            void assertMessageCount(int expected) {
                ASSERT(_messageCount == expected);
            }

        private:
            int _messageCount;
            std::unique_ptr<NetworkInterfaceASIO> _net;
        };

        TEST_F(ReplTestASIO, DummyTest) {
            int runs = 1;
            init();

            NetworkInterfaceASIO* net = getNet();
            const ReplicationExecutor::RemoteCommandRequest request(HostAndPort("localhost", 12345),
                                                                    "somedb",
                                                                    BSON("hello" << "world"));
            for (int i = 0; i < runs; i++) {
                net->startCommand(ReplicationExecutor::CallbackHandle(),
                                  request,
                                  stdx::bind(&ReplTestASIO::receiveMessage, this, ResponseStatus(Status::OK())));
            }

            waitForMessageCount(runs);
            net->shutdown();
        }

    } // namespace repl
} // namespace mongo
