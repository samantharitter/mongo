/*    Copyright 2015 MongoDB Inc.
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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#pragma once

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include <thread>

#include "asio.hpp"

#include "mongo/platform/basic.h"

#include "mongo/util/net/message_port.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/listen.h"

namespace mongo {

   using asio::ip::tcp;

   class ASIOMessageServer : public MessageServer {
   public:
      ASIOMessageServer(const MessageServer::Options& opts, MessageHandler* handler=nullptr) {
         std::cout << "ASIOMessageServer: congrats you constructed an ASIOMessageServer\n";
      }

      // don't use this with ASIO noop.
      virtual void accepted(boost::shared_ptr<Socket> psocket, long long connectionId);

      virtual void setAsTimeTracker() {
         std::cout << "ASIOMessageServer: setting as time tracker...JK TOTALLY NOT DOING THAT AHAHA\n";
      }

      virtual void setupSockets() {
         std::cout << "ASIOMessageServer: setting up sockets, *hypothetically*\n";
      }

      void run();

      virtual bool useUnixSockets() const {
         std::cout << "ASIOMessageServer: unix sockets??\n";
         return true;
      }

   private:
      asio::io_service _io_service;
      std::thread _serviceRunner;
   };

} // namespace mongo
