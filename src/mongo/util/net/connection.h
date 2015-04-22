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

// borrowed heavily from Christopher Kohlhoff's example class at
// https://github.com/chriskohlhoff/asio/blob/master/asio/src/examples/cpp11/http/server/connection.hpp

// Copyright (c) 2003-2015 Christopher M. Kohlhoff (chris at kohlhoff dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#pragma once

#include <array>
#include <memory>
#include "asio.hpp"
#include "mongo/util/net/hostandport.h"

namespace mongo {
   namespace net {

      using asio::ip::tcp;

      class Connection : public std::enable_shared_from_this<Connection> {
      public:

         // why do this instead of inheriting non copyable?
          Connection(const Connection&) = delete; // no copy construction
          Connection& operator=(const Connection&) = delete; // no copy assignment

          explicit Connection(tcp::socket sock);
          explicit Connection(const HostAndPort& addr);

         // this method calls doRead
          void startWorking();
          void stopWorking();

      private:

         // calls async_read_some, takes a lambda
         // reads into a buffer, then passes buffer to request_parser
         // if that works, then calls handler to handle the request
         // then we begin to read
          void connectSocketFromHostAndPort(const HostAndPort& addr);
          void doRead();
          void doWrite();

          // iop service (do we need to store this?)
          asio::io_service _service;

          // our socket
          tcp::socket _socket;
      };
   } // namespace server
} // namespace http
