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

        MockSocketASIO::MockSocketASIO(asio::io_service& io_service) {}

        std::size_t MockSocketASIO::_send(const asio::const_buffer& buf) {
            std::cout << "mock send\n";
            _sent.push(buf);
            return asio::buffer_size(_sent.front());
        }

        std::size_t MockSocketASIO::_recv(const asio::mutable_buffer& buf) {
            std::cout << "mock receive\n";
            if (!_toReceive.empty()) {
                asio::buffer_copy(buf, _toReceive.front());
                _toReceive.pop();
                return asio::buffer_size(buf);
            }
            return 0;
        }

        void MockSocketASIO::pushRecv(const asio::const_buffer&& buf /* IN */) {
            std::cout << "pushRecv()\n";
            _toReceive.push(std::move(buf));
        }

        std::size_t MockSocketASIO::pullSent(const asio::mutable_buffer& buf /* OUT */) {
            std::cout << "pullSent()\n";
            if (!_sent.empty()) {
                const asio::const_buffer temp = _sent.front();
                asio::buffer_copy(buf, temp);
                _sent.pop();
                return asio::buffer_size(temp);
            }
            return 0;
        }

        // utility function to convert Message class to bytes?
        void MockSocketASIO::messageToBuf(const Message& m,
                                          const asio::mutable_buffer& buf /* OUT */) {
            std::cout << "messageToBuf()\n";
        }
    } // namespace net
} // namespace mongo
