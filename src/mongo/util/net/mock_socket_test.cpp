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

#include "mongo/unittest/unittest.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/net/mock_socket.h"
#include "mongo/util/net/socket_interface.h"

namespace mongo {
    namespace net {

        class MockRemote {
        public:
            MockRemote(SocketInterface* socket) : _socket(socket) {}

            std::size_t send(asio::const_buffer& buf) {
                return _socket->send(buf);
            }

            std::size_t receive(asio::mutable_buffer& buf) {
                return _socket->receive(buf);
            }

        private:
            SocketInterface* _socket;
        };

        TEST(MockSocket, Basic) {
            asio::io_service io_service;
            MockSocketASIO socket(io_service);
            MockRemote remote(&socket);
            std::size_t res;
            int data_len = 128;
            char data[data_len];

            // call pullSent(), ensure we get nothing and 0
            asio::mutable_buffer sent_buf(data, data_len);
            res = socket.pullSent(sent_buf);
            ASSERT(res == 0);

            // call send() with a message, then pull sent, get message
            std::string message = "hello!";
            asio::const_buffer msg_buf(message.c_str(), message.length());
            res = remote.send(msg_buf);
            ASSERT(res == message.length());
            res = socket.pullSent(sent_buf);
            ASSERT(res == message.length());

            // call receive(), make sure we get nothing
            asio::mutable_buffer recv_buf;
            res = remote.receive(recv_buf);
            ASSERT(res == 0);

            // call pushRecv(), then receive, make sure we get reply
            std::string reply = "hi!";
            asio::const_buffer reply_buf(reply.c_str(), reply.length());
            socket.pushRecv(std::move(reply_buf));
            res = remote.receive(recv_buf);
        }

    } // namespace net
} // namespace mongo
