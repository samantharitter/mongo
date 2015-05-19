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

#include "mongo/client/connection_pool.h"
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
        AsyncOp(const CommandData&& cmd, Date_t now, asio::io_service* service, ConnectionPool* pool) :
           _canceled(false),
              md(NULL),
              _start(now),
              _cmd(cmd),
              _service(service),
              _pool(pool)
              {
                 // nothing
              }

           bool connect(Date_t now) {
              try {
                 _conn.reset(new ConnectionPool::ConnectionPtr(_pool, _cmd.request.target, now, Milliseconds(10000)));
                 _sock.reset(new tcp::socket(*_service, asio::ip::tcp::v6(), _conn->get()->p->psock->rawFD()));
                 return true;
              } catch (const std::exception& e) {
                 std::cout << "ASYNC_OP: exception occurred in connect()\n";
                 return false;
              }
           }

           bool disconnect(Date_t now) {
              _conn->done(now, true);
              return true;
           }

           tcp::socket* sock() {
              return _sock.get();
           }

           bool _canceled;

           Message toSend;
           Message toRecv;
           MSGHEADER::Value header; // todo clean up
           char *md;

           BSONObj output;
           const Date_t _start;
           CommandData _cmd;
           asio::io_service* _service;

           // yeahhh...
           ConnectionPool* _pool;
           std::unique_ptr<ConnectionPool::ConnectionPtr> _conn;
           std::unique_ptr<tcp::socket> _sock;
        };
        typedef const boost::shared_ptr<AsyncOp> sharedAsyncOp;
        typedef stdx::list<AsyncOp*> AsyncOpList;

        void _killTime();

        void _runCommand(const CommandData&& cmd);
        void _asyncRunCmd(const CommandData&& cmd);

        // todo make free function
        void _messageFromRequest(const ReplicationExecutor::RemoteCommandRequest& request,
                                 Message& toSend);

        void _asyncSendSimpleMessage(sharedAsyncOp op,
                                     const asio::const_buffer& buf);

        void _asyncSendComplicatedMessage(sharedAsyncOp op,
                                          std::vector<std::pair<char*, int>> data,
                                          std::vector<std::pair<char*, int>>::const_iterator i);

        void _completedWriteCallback(sharedAsyncOp op);
        void _networkErrorCallback(sharedAsyncOp op, std::error_code ec);
        OperationContext* createOperationContext() override;

        asio::io_service _io_service;
        asio::steady_timer _timer;

        std::thread _serviceRunner;

        bool _shutdown;

      private:

        void _completeOperation(sharedAsyncOp op);
        void _validateMessageHeader(sharedAsyncOp op);
        void _keepAlive(sharedAsyncOp op);
        void _recvMessageHeader(sharedAsyncOp op);
        void _recvMessageBody(sharedAsyncOp op);
        void _receiveResponse(sharedAsyncOp op);
        void _signalWorkAvailable_inlock();

        bool _isExecutorRunnable;
        boost::mutex _mutex;

        // for executor.
        boost::condition_variable _isExecutorRunnableCondition;

        // to steal from
        boost::scoped_ptr<ConnectionPool> _connPool;

        // for canceling. These ptrs are NON-owning.
        AsyncOpList _inProgress;
      };

   } // namespace repl
} // namespace mongo
