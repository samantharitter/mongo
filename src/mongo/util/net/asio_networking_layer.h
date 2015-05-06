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

#include "mongo/platform/basic.h"

#include <thread>

#include "mongo/util/net/message.h"

#include "asio.hpp"

namespace mongo {

   using asio::ip::tcp;

   typedef boost::shared_ptr<tcp::socket> SharedSocket;

   class AsyncMessageRunner {
   public:
      AsyncMessageRunner();

      // todo: this can take hostandport and handle responses
      void process(Message& m, int id);

      void startup() {
         _serviceRunner = std::thread([this] {
               asio::io_service::work work(_service);
               _service.run();
            });
      }

      void shutdown() {
         _service.stop();
         _serviceRunner.join();
      }

   private:
      asio::io_service _service;
      std::thread _serviceRunner;
   };

   class AsyncNetworkingLayer {
   public:

      AsyncNetworkingLayer(int port);

      // todo: handle for message runner (db side) to send responses?
      // ask Andrew

      void startup();
      void shutdown();

   private:

      void do_accept();
      void do_stuff();

      // choice:
      // - should sockets be passed along and kind of float on sm?
      // - or should there be a socket table where they are stored by
      //   connection id?
      // - for introspection by higher levels perhaps the latter would be best
      // - also, if db needs to be able to send directly, maybe the latter?

      // per-connection "state machine":
      // need to expand into recv-header and recv-body
      void newConnection(SharedSocket sock);
      void recvMessageHeader(SharedSocket sock, int id);
      void recvMessageBody(SharedSocket sock,
                           int id,
                           boost::shared_ptr<MSGHEADER::Value> header,
                           int headerLen);
      void processMessage(SharedSocket sock, int id, Message& m);
      void cmdFinished(SharedSocket sock, int id);

      asio::io_service _service;
      tcp::acceptor _acceptor;
      asio::steady_timer _timer;
      int _port;
      int _connections;

      AsyncMessageRunner _db;

      std::thread _serviceRunner;
   };
} // namespace mongo
