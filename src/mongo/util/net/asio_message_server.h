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
#include <boost/shared_ptr.hpp>

#include "mongo/platform/basic.h"

#include "mongo/db/dbmessage.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/listen.h"

namespace mongo {

   using asio::ip::tcp;

   typedef boost::shared_ptr<tcp::socket> StickySocket;


   /*
    * The ASIOMessageServer encodes state machines to handle connections.
    * Each step in a state machine is represented as a task that is posted to
    * the io service and executed asynchronously. Upon completion the current
    * step posts the next step to the io service.
    *
    * Each connection runs its own state machine with the following states:
    *
    * 0: accept connection
    * 1: receive message header, validate
    * 2: receive message body
    * 3: send to database
    *    - if response, continue to 4
    *    - if not, back to 1
    * 4: send db response
    *    - if getmore needed, continue to 5
    *    - if not, back to 1
    * 5: run getmore command, send db response (perform this state 0 or more times until exhausted)
    *    - back to 4
    * 6: close connection. If other states error we end up here.
    *
    * There's probably a better way to represent this as a graph:
    *
    *                  /<-------\
    * 0 |-> 1 -> 2 -> 3 -> 4 -> 5 |-> 6
    *        \\<------/    /
    *         \<----------/
    */

   // todo: refactor to use MessageHandler abstraction

   class ASIOMessageServer : public MessageServer {
   public:
   ASIOMessageServer(const MessageServer::Options& opts, MessageHandler* handler=nullptr)
      : _shutdown(false),
         _port(opts.port),
         _service(),
         _acceptor(_service)
         {
         std::cout << "ASIOMessageServer: congrats you constructed an ASIOMessageServer\n";
      }

      // don't use this with ASIO noop.
      virtual void accepted(boost::shared_ptr<Socket> psocket, long long connectionId) {
        std::cout << "ASIOMessageServer: accepted()\n";
      }

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

      class Connection : public AbstractMessagingPort {
      public:
      Connection(StickySocket sock)
         : md(nullptr),
            _sock(sock)
            {}

         tcp::socket* sock() {
            return _sock.get();
         }

         // these are required of AbstractMessagingPorts
         virtual void reply(Message& received, Message& response, MSGID responseTo) {
            std::cout << "reply not implemented\n";
         }
         virtual void reply(Message& received, Message& response) {
            std::cout << "reply not implemented\n";
         }

         virtual unsigned remotePort() const {
            return _sock.get()->remote_endpoint().port();
         }

         virtual HostAndPort remote() const {
            tcp::endpoint endpoint = _sock.get()->remote_endpoint();
            asio::ip::address address = endpoint.address(); // IP address
            std::ostringstream stream;
            stream << address;
            std::string hostname = stream.str();
            return HostAndPort(hostname, endpoint.port());
         }

         virtual SockAddr remoteAddr() const {
            tcp::endpoint endpoint = _sock.get()->remote_endpoint();
            asio::ip::address address = endpoint.address(); // IP address
            std::ostringstream stream;
            stream << address;
            return SockAddr(stream.str().c_str(), endpoint.port());
         }

         virtual SockAddr localAddr() const {
            return SockAddr(_sock.get()->local_endpoint().port());
         }

         Message toSend;
         Message toRecv;
         MSGHEADER::Value header; // eh
         char *md;

         StickySocket _sock;
      };

      typedef boost::shared_ptr<Connection> ClientConnection;

   private:

      /* STATE 0 */
      void _doAccept();

      /* STATE 1 */
      void _handleIncomingMessage(ClientConnection conn);
      void _recvMessageHeader(ClientConnection conn);

      /* STATE 2 */
      void _recvMessageBody(ClientConnection conn);

      /* STATE 3 */
      void _process(ClientConnection conn);
      void _processSync(ClientConnection conn);
      void _processAsync(ClientConnection conn);

      /* STATE 4 */
      void _sendDatabaseResponse(ClientConnection conn, DbResponse dbresponse);

      /* STATE 5 */
      void _runGetMore(ClientConnection conn, DbResponse dbresponse);

      /* STATE 6 */
      void _networkError(ClientConnection conn, std::error_code ec);

      HostAndPort _getRemote(ClientConnection conn);

      bool _shutdown;
      int _port;

      asio::io_service _service;
      tcp::acceptor _acceptor;

      std::thread _serviceRunner;
   };

} // namespace mongo
