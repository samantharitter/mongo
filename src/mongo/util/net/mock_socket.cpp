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

#include "mock_socket.h"

namespace mongo {
    namespace net {

        MockSocketASIO::MockSocketASIO() {}

        // TODO: could we somehow override send() and recv()?

        // have a send queue onto which we can push messages
        void MockSocketASIO::pushSend(const asio::const_buffer&& buf /* IN */) {
            std::cout << "pushSend()\n";
            _toSend.push(std::move(buf));
        }

        // have a receive queue from which we can read messages
        // returns true if we popped, false if we didn't (was empty)
        bool MockSocketASIO::pullRecv(const asio::mutable_buffer& buf /* OUT */) {
            std::cout << "pullRecv()";
            if (!_recvd.empty()) {
                asio::buffer_copy(buf, _recvd.front());
                _recvd.pop();
                return true;
            }
            return false;
        }

        // utility function to convert Message class to bytes?
        void MockSocketASIO::messageToBuf(const Message& m,
                                          const asio::mutable_buffer& buf /* OUT */) {
            std::cout << "messageToBuf()";
        }
    } // namespace net
} // namespace mongo
