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
#include <system_error>
#include <thread>

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

        class AsyncOp;
        using SharedAsyncOp = const std::shared_ptr<AsyncOp>;
        using AsyncOpList = stdx::list<AsyncOp*>;

        struct CommandData;

    private:

        void _asyncRunCommand(CommandData&& cmd);

        void _messageFromRequest(const RemoteCommandRequest& request,
                                 Message* toSend);

        void _asyncSendSimpleMessage(const SharedAsyncOp& op,
                                     const asio::const_buffer& buf);

        void _asyncSendVectorMessage(const SharedAsyncOp& op,
                                     const std::vector<std::pair<char*, int>>& data,
                                     std::vector<std::pair<char*, int>>::const_iterator i);

        void _completedWriteCallback(const SharedAsyncOp& op);
        void _networkErrorCallback(const SharedAsyncOp& op, const std::error_code& ec);

        asio::io_service _io_service;
        asio::steady_timer _timer;

        std::thread _serviceRunner;

        std::atomic_bool _shutdown;

      private:

        void _completeOperation(const SharedAsyncOp& op);
        void _validateMessageHeader(const SharedAsyncOp& op);
        void _keepAlive(const SharedAsyncOp& op);
        void _recvMessageHeader(const SharedAsyncOp& op);
        void _recvMessageBody(const SharedAsyncOp& op);
        void _receiveResponse(const SharedAsyncOp& op);
        void _signalWorkAvailable_inlock();

        bool _isExecutorRunnable;
        stdx::mutex _mutex;
        stdx::condition_variable _isExecutorRunnableCondition;

        std::unique_ptr<ConnectionPool> _connPool;

        // for canceling. These ptrs are NON-owning.
        AsyncOpList _inProgress;
      };

} // namespace executor
} // namespace mongo
