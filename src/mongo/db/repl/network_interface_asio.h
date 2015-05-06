/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include <system_error>

#include "asio.hpp"

#include <thread>

#include "mongo/db/repl/replication_executor.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/message.h"

namespace mongo {
   namespace repl {

      using asio::ip::tcp;

      /**
       * This is a test implementation of the replication network interface using
       * Christopher Kohlhoff's ASIO networking library.
       */
      class NetworkInterfaceASIO : public ReplicationExecutor::NetworkInterface,
         public std::enable_shared_from_this<NetworkInterfaceASIO> {
      public:
        explicit NetworkInterfaceASIO();
        virtual ~NetworkInterfaceASIO();
        virtual std::string getDiagnosticString();
        virtual void startup();
        virtual void shutdown();
        virtual void waitForWork();
        virtual void waitForWorkUntil(Date_t when);
        virtual void signalWorkAvailable();
        virtual Date_t now();
        virtual void startCommand(
                const ReplicationExecutor::CallbackHandle& cbHandle,
                const ReplicationExecutor::RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish);
        virtual void cancelCommand(const ReplicationExecutor::CallbackHandle& cbHandle);
        virtual void runCallbackWithGlobalExclusiveLock(
                const stdx::function<void (OperationContext*)>& callback);

      private:

        /**
         * Information describing an in-flight command.
         */
        struct CommandData {
           ReplicationExecutor::CallbackHandle cbHandle;
           ReplicationExecutor::RemoteCommandRequest request;
           RemoteCommandCompletionFn onFinish;
        };
        typedef stdx::list<CommandData> CommandDataList;

        /**
         * A helper type for async networking operations
         */
        class AsyncOp {
        public:
        AsyncOp(const CommandData&& cmd, Date_t now, asio::io_service* service) :
           _start(now),
              _cmd(cmd),
              _service(service),
              _sock(*_service)
              {
                 // connect socket
                 // TODO use a non-blocking connect, store network state
                 HostAndPort addr = _cmd.request.target;
                 tcp::resolver resolver(*_service);
                 asio::connect(_sock, resolver.resolve({addr.host(), std::to_string(addr.port())}));
              }

           //~AsyncOp() {}

           const Date_t _start;
           CommandData _cmd;
           asio::io_service* _service;
           tcp::socket _sock;
        };

        void _killTime();

        void _runCommand(const CommandData&& cmd);
        void _asyncRunCmd(const CommandData&& cmd);

        // todo make free function
        void _messageFromRequest(const ReplicationExecutor::RemoteCommandRequest& request,
                                 Message& toSend);

        void _asyncSendSimpleMessage(const boost::shared_ptr<AsyncOp> op,
                                     const asio::const_buffer& buf);

        void _asyncSendComplicatedMessage(const boost::shared_ptr<AsyncOp> op,
                                          std::vector<std::pair<char*, int>> data,
                                          std::vector<std::pair<char*, int>>::const_iterator i);

        void _completedWriteCallback(const boost::shared_ptr<AsyncOp> op);
        void _networkErrorCallback(const boost::shared_ptr<AsyncOp> op, std::error_code ec);

        asio::io_service _io_service;
        asio::steady_timer _timer;

        std::thread _serviceRunner;
        std::set<AsyncOp> _active_ops;

        bool _shutdown;
      };

   } // namespace repl
} // namespace mongo
