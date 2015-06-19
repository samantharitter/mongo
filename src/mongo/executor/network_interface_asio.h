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

#include <asio.hpp>
#include <atomic>
#include <system_error>
#include <thread>
#include <unordered_map>

#include "mongo/client/connection_pool.h"
#include "mongo/client/remote_command_runner.h"
#include "mongo/executor/network_interface.h"
#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/net/message.h"

namespace mongo {
namespace executor {

    /**
     * Implementation of the replication system's network interface using Christopher
     * Kohlhoff's ASIO library instead of existing MongoDB networking primitives.
     */
    class NetworkInterfaceASIO final : public NetworkInterface {
    public:

       enum class State {
          READY,
          RUNNING,
          SHUTDOWN
       };

        NetworkInterfaceASIO();
        std::string getDiagnosticString() override;
        void startup() override;
        void shutdown() override;
        void waitForWork() override;
        void waitForWorkUntil(Date_t when) override;
        void signalWorkAvailable() override;
        Date_t now() override;
        void startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                                  const RemoteCommandRequest& request,
                                  const RemoteCommandCompletionFn& onFinish) override;
        void cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) override;

        bool inShutdown();

    private:

        /**
         * Information describing an in-flight command.
         */
        struct CommandData {
           TaskExecutor::CallbackHandle cbHandle;
           RemoteCommandRequest request;
           RemoteCommandCompletionFn onFinish;
        };

        /**
         * Helper object to manage individual network operations.
         */
        class AsyncOp {
        public:

            AsyncOp(CommandData&& cmdObj, Date_t now, asio::io_service* service,
                    ConnectionPool* pool) :
                start(now),
                cmd(std::move(cmdObj)),
                _state(OpState::READY),
                _service(service),
                _pool(pool)
                {}

           enum class OpState {
              READY,
              CONNECTED,
              DISCONNECTED,
              CANCELED,
              COMPLETED
           };

           void connect(Date_t now);
           bool connected();
           bool complete();
           void disconnect(Date_t now);
           void cancel();
           bool canceled();
           asio::ip::tcp::socket* sock();

           Message toSend;
           Message toRecv;
           MSGHEADER::Value header;

           // this is owned by toRecv, freed by toRecv.reset()
           char* toRecvBuf;

           BSONObj output;
           const Date_t start;
           CommandData cmd;

        private:
           std::atomic<OpState> _state;

           asio::io_service* const _service;
           ConnectionPool* const _pool;

           std::unique_ptr<ConnectionPool::ConnectionPtr> _conn;
           std::unique_ptr<asio::ip::tcp::socket> _sock;
        };


        void _asyncRunCommand(CommandData&& cmd);

        void _messageFromRequest(const RemoteCommandRequest& request,
                                 Message* toSend,
                                 bool useOpCommand = false);

        void _asyncSendSimpleMessage(AsyncOp* op,
                                     const asio::const_buffer& buf);

        void _completedWriteCallback(AsyncOp* op);
        void _networkErrorCallback(AsyncOp* op, const std::error_code& ec);

        asio::io_service _io_service;

        std::thread _serviceRunner;

        std::atomic<State> _state;

      private:

        void _completeOperation(AsyncOp* op);
        void _validateMessageHeader(AsyncOp* op);
        void _keepAlive(AsyncOp* op);
        void _recvMessageHeader(AsyncOp* op);
        void _recvMessageBody(AsyncOp* op);
        void _receiveResponse(AsyncOp* op);
        void _signalWorkAvailable_inlock();

        stdx::mutex _inProgressMutex;
        stdx::mutex _executorMutex;
        bool _isExecutorRunnable;
        stdx::condition_variable _isExecutorRunnableCondition;

        std::unique_ptr<ConnectionPool> _connPool;

        std::unordered_map<AsyncOp*, std::unique_ptr<AsyncOp>> _inProgress;
      };

} // namespace executor
} // namespace mongo
