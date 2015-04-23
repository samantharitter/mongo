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

#pragma once

#include <queue>

#include "asio.hpp"

#include "mongo/util/net/message.h"
#include "mongo/util/net/socket_interface.h"

namespace mongo {
   namespace net {

      using asio::ip::tcp;

      // tcp::socket -> basic_stream_socket<tcp>
      class MockSocketASIO : public SocketInterface {
      public:

         MockSocketASIO(asio::io_service& io_service);

         // need to inherit from tcp::socket, stub out basic socket functions:

         // override send methods
         std::size_t send(const asio::const_buffer& buf) { return _send(buf); }
         std::size_t send(const asio::const_buffer& buf,
                          asio::socket_base::message_flags flags) { return _send(buf); }
         std::size_t send(const asio::const_buffer& buf,
                          asio::socket_base::message_flags flags,
                          asio::error_code& ec) { return _send(buf); }

         // recv
         std::size_t receive(const asio::mutable_buffer& buf) { return _recv(buf); }
         std::size_t receive(const asio::mutable_buffer& buf,
                             asio::socket_base::message_flags flags) { return _recv(buf); }
         std::size_t receive(const asio::mutable_buffer& buf,
                             asio::socket_base::message_flags flags,
                             asio::error_code* ec) { return _recv(buf); }

         // async_send
         // async_recv

         // have a queue onto which we can push messages remote will receive
         void pushRecv(const asio::const_buffer&& buf /* IN */);

         // have a receive queue from which we can read messages remote has sent
         std::size_t pullSent(const asio::mutable_buffer& buf /* OUT */);

         // utility function to convert Message class to bytes?
         void messageToBuf(const Message& m, const asio::mutable_buffer& buf /* OUT */);

      private:
         std::size_t _send(const asio::const_buffer& buf);
         std::size_t _recv(const asio::mutable_buffer& buf);

         std::queue<asio::const_buffer> _sent;
         std::queue<asio::const_buffer> _toReceive;
      };

   } // namespace net
} // namespace mongo
