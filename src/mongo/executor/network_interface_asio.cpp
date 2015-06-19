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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kExecutor

#include "mongo/platform/basic.h"

#include <chrono>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/client.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/executor/network_interface_asio.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"
#include "mongo/util/net/sock.h"

namespace mongo {
namespace executor {

    using ResponseStatus = TaskExecutor::ResponseStatus;
    using asio::ip::tcp;
    using RemoteCommandCompletionFn =
        stdx::function<void (const ResponseStatus&)>;

    // throws if any connection issues occur
    void NetworkInterfaceASIO::AsyncOp::connect(Date_t now) {
        _conn = stdx::make_unique<ConnectionPool::ConnectionPtr>(_pool,
                                                                 cmd.request.target,
                                                                 now,
                                                                 Milliseconds(10000));
        _state.store(OpState::CONNECTED);

        // Detect protocol used in underlying socket
        int protocol = _conn->get()->port().localAddr().getType();
        if (protocol != AF_INET &&
            protocol != AF_INET6) {
            throw SocketException(SocketException::CONNECT_ERROR, "Unsupported family");
        }

        // TODO: should there be some intermediate state, CONNECTION_ACQUIRED,
        // that we go to before making the ASIO socket? In case this is where we fail?
        _sock = stdx::make_unique<tcp::socket>(*_service,
                                               protocol == AF_INET ? tcp::v4() : tcp::v6(),
                                               _conn->get()->port().psock->rawFD());
    }

    void NetworkInterfaceASIO::AsyncOp::cancel() {
        // An operation may be in mid-flight when it is canceled, so we
        // do not disconnect upon cancellation.
        _state.store(OpState::CANCELED);
    }

    // todo: check for cancellation
    bool NetworkInterfaceASIO::AsyncOp::canceled() {
        return (_state.load() == OpState::CANCELED);
    }

    bool NetworkInterfaceASIO::AsyncOp::connected() {
        OpState state = _state.load();
        return (state == OpState::CONNECTED ||
                state == OpState::CANCELED);
    }

    // Performs an atomic exchange on AsyncOp's state,
    // returns 'true' if we are the first thread to complete op
    bool NetworkInterfaceASIO::AsyncOp::complete() {
        return (_state.exchange(OpState::COMPLETED) == OpState::COMPLETED);
    }

    void NetworkInterfaceASIO::AsyncOp::disconnect(Date_t now) {
        if (!connected()) {
            return;
        }

        _state.store(OpState::DISCONNECTED);
        _conn->done(now, true);
        toSend.reset();
        toRecv.reset();
    }

    tcp::socket* NetworkInterfaceASIO::AsyncOp::sock() {
        return _sock.get();
    }

    NetworkInterfaceASIO::NetworkInterfaceASIO() :
        _io_service(),
        _state(State::READY),
        _isExecutorRunnable(false) {
        _connPool = stdx::make_unique<ConnectionPool>(kMessagingPortKeepOpen);
    }

    std::string NetworkInterfaceASIO::getDiagnosticString() {
        str::stream output;
        output << "NetworkInterfaceASIO";
        output << " inShutdown: " << inShutdown();
        return output;
    }

    void NetworkInterfaceASIO::_messageFromRequest(const RemoteCommandRequest& request,
                                                   Message* toSend,
                                                   bool useOpCommand) {
        BSONObj query = request.cmdObj;
        invariant(query.isValid());

        // TODO: once OP_COMMAND work is complete,
        // look at client to see if it supports OP_COMMAND.

        // TODO: investigate whether we can use CommandRequestBuilder here.

        BufBuilder b;
        b.appendNum(0); // opts
        b.appendStr(request.dbname + ".$cmd");
        b.appendNum(0); // toSkip
        b.appendNum(1); // toReturn, don't care about responses
        query.appendSelfToBufBuilder(b);

        // TODO: if AsyncOp can own this buffer, we can avoid copying it in setData().
        toSend->setData(dbQuery, b.buf(), b.len());
        toSend->header().setId(nextMessageId());
        toSend->header().setResponseTo(0);
    }

    void NetworkInterfaceASIO::_asyncSendSimpleMessage(AsyncOp* op,
                                                       const asio::const_buffer& buf) {
        asio::async_write(*(op->sock()), asio::buffer(buf),
            [this, op](std::error_code ec, std::size_t bytes) {
                 if (ec) {
                     return _networkErrorCallback(op, ec);
                 } else if (op->canceled()) {
                     return _completeOperation(op);
                 } else {
                     _receiveResponse(op);
                 }
            });
    }

    void NetworkInterfaceASIO::_receiveResponse(AsyncOp* op) {
        _recvMessageHeader(op);
    }

    void NetworkInterfaceASIO::_validateMessageHeader(AsyncOp* op) {
        // TODO: this error code should be more meaningful.
        std::error_code ec;

        // validate message length
        int len = op->header.constView().getMessageLength();
        if (len == 542393671) {
            LOG(3) << "attempt to access MongoDB over HTTP on the native driver port.\n";
            return _networkErrorCallback(op, ec);
        } else if (len == -1) {
            // TODO: an endian check is run after the client connects, we should
            // set that we've received the client's handshake
            LOG(3) << "Endian check received from client\n";
            return _networkErrorCallback(op, ec);
        } else if (static_cast<size_t>(len) < sizeof(MSGHEADER::Value) ||
                   static_cast<size_t>(len) > MaxMessageSizeBytes) {
            LOG(0) << "recv(): message len " << len << " is invalid. "
                   << "Min " << sizeof(MSGHEADER::Value) << " Max: " << MaxMessageSizeBytes;
            return _networkErrorCallback(op, ec);
        }

        // validate response id
        size_t expectedId = op->toSend.header().getId();
        size_t actualId = op->header.constView().getResponseTo();
        if (actualId != expectedId) {
            LOG(3) << "got wrong response:"
                   << "\n   expected response id: " << expectedId
                   << "\n   instead got response id: " << actualId << "\n";
            return _networkErrorCallback(op, ec);
        }
        _recvMessageBody(op);
    }

    void NetworkInterfaceASIO::_recvMessageBody(AsyncOp* op) {
        // len = whole message length, data + header
        int len = op->header.constView().getMessageLength();

        int z = (len+1023)&0xfffffc00;
        invariant(z>=len);
        op->toRecvBuf = reinterpret_cast<char *>(mongoMalloc(z));
        MsgData::View md_view = op->toRecvBuf;

        // copy header data into master buffer
        int headerLen = sizeof(MSGHEADER::Value);
        memcpy(md_view.view2ptr(), &op->header, headerLen);
        int bodyLength = len - headerLen;

        // receive remaining data into md->data
        asio::async_read(*(op->sock()), asio::buffer(md_view.data(), bodyLength),
            [this, op, md_view](asio::error_code ec, size_t bytes) {
                if (ec) {
                    LOG(3) << "error receiving message body\n";
                    return _networkErrorCallback(op, ec);
                } else if (op->canceled()) {
                    return _completeOperation(op);
                } else {
                    op->toRecv.setData((char *)md_view.view2ptr(), true);
                    return _completedWriteCallback(op);
                }
        });
    }

    void NetworkInterfaceASIO::_recvMessageHeader(AsyncOp* op) {
        asio::async_read(*(op->sock()),
                         asio::buffer(reinterpret_cast<char *>(&op->header),
                                      sizeof(MSGHEADER::Value)),
                         [this, op](asio::error_code ec, size_t bytes) {
                             if (ec) {
                                 LOG(3) << "error receiving header\n";
                                 return _networkErrorCallback(op, ec);
                             } else if (op->canceled()) {
                                 return _completeOperation(op);
                             } else {
                                 return _validateMessageHeader(op);
                             }
                         });
        }

    void NetworkInterfaceASIO::_completedWriteCallback(AsyncOp* op) {
        if (op->toRecv.empty()) {
            op->output = BSONObj();
            LOG(3) << "received an empty message\n";
        } else {
            QueryResult::View qr = op->toRecv.singleData().view2ptr();
            // unavoidable copy
            op->output = BSONObj(qr.data()).getOwned();
        }
        _completeOperation(op);
    }

    void NetworkInterfaceASIO::_networkErrorCallback(AsyncOp* op,
                                                     const std::error_code& ec) {
        if (op->toRecv._buf) {
            QueryResult::View qr = op->toRecv.singleData().view2ptr();
            LOG(3) << "networking error receiving part of message "
                   << op->toRecv.header().getId() << "\n";
            op->output = BSONObj(qr.data());
        } else if (op->toRecv.empty()) {
            // TODO: we may need to handle vector messages separately.
            LOG(3) << "networking error occurred, toRecv is empty\n";
            op->output = BSONObj();
        }

        _completeOperation(op);
    }

    // NOTE: this method may only be called by ASIO threads
    // (do not call from methods entered by ReplicationExecutor threads)
    void NetworkInterfaceASIO::_completeOperation(AsyncOp* op) {
        const Date_t end = now();
        bool canceled = op->canceled();
        op->disconnect(end);
        if (op->complete()) {
            return;
        }

        if (canceled) {
            op->cmd.onFinish(ResponseStatus(ErrorCodes::CallbackCanceled, "Callback canceled"));
        } else {
            op->cmd.onFinish(ResponseStatus(Response(op->output, Milliseconds(end - op->start))));
        }

        {
            stdx::lock_guard<stdx::mutex> lk(_inProgressMutex);
            _inProgress.erase(op);
        }

        signalWorkAvailable();
    }

    void NetworkInterfaceASIO::_asyncRunCommand(CommandData&& cmd) {
        LOG(3) << "running command " << cmd.request.cmdObj
               << " against database " << cmd.request.dbname
               << " across network to " << cmd.request.target.toString() << "\n";

        if (inShutdown()) {
            return;
        }

        auto ownedOp = stdx::make_unique<AsyncOp>(std::move(cmd),
                                                  now(),
                                                  &_io_service,
                                                  _connPool.get());

        AsyncOp* op = ownedOp.get();

        {
            stdx::lock_guard<stdx::mutex> lk(_inProgressMutex);
            _inProgress.emplace(op, std::move(ownedOp));
        }

        // auth in a separate thread to avoid blocking the rest of the system
        std::thread t([this, op]() {
                try {
                    op->connect(now());
                } catch (...) {
                    LOG(3) << "connect() failed, posting mock completion\n";

                    if (inShutdown()) {
                        return;
                    }

                    ResponseStatus status(exceptionToStatus());
                    asio::post(_io_service, [this, op, status]() {
                            return _completeOperation(op);
                        });
                    return;
                }

                // send control back to main thread(pool)
                asio::post(_io_service, [this, op]() {
                        _messageFromRequest(op->cmd.request, &op->toSend);
                        if (op->toSend.empty()) {
                            _completedWriteCallback(op);
                        } else if (op->canceled()) {
                            return _completeOperation(op);
                        } else {
                            // TODO: some day we may need to support vector messages.
                            fassert(28693, op->toSend._buf != 0);
                            asio::const_buffer buf(op->toSend._buf, op->toSend.size());
                            return _asyncSendSimpleMessage(op, buf);
                        }
                    });
            });
        t.detach();
    }

    void NetworkInterfaceASIO::startup() {
        _serviceRunner = std::thread([this]() {
                asio::io_service::work work(_io_service);
                _io_service.run();
            });
        _state.store(State::RUNNING);
    }

    bool NetworkInterfaceASIO::inShutdown() {
        return (_state.load() == State::SHUTDOWN);
    }

    void NetworkInterfaceASIO::shutdown() {
        _state.store(State::SHUTDOWN);
        _io_service.stop();
        _serviceRunner.join();
    }

    void NetworkInterfaceASIO::signalWorkAvailable() {
        stdx::unique_lock<stdx::mutex> lk(_executorMutex);
        _signalWorkAvailable_inlock();
    }

    void NetworkInterfaceASIO::_signalWorkAvailable_inlock() {
        if (!_isExecutorRunnable) {
            _isExecutorRunnable = true;
            _isExecutorRunnableCondition.notify_one();
        }
    }

    void NetworkInterfaceASIO::waitForWork() {
        stdx::unique_lock<stdx::mutex> lk(_executorMutex);
        while (!_isExecutorRunnable) {
            _isExecutorRunnableCondition.wait(lk);
        }
        _isExecutorRunnable = false;
    }

    void NetworkInterfaceASIO::waitForWorkUntil(Date_t when) {
        stdx::unique_lock<stdx::mutex> lk(_executorMutex);
        while (!_isExecutorRunnable) {
            const Milliseconds waitTime(when - now());
            if (waitTime <= Milliseconds(0)) {
                break;
            }
            _isExecutorRunnableCondition.wait_for(lk, waitTime);
        }
        _isExecutorRunnable = false;
    }

    Date_t NetworkInterfaceASIO::now() {
        return Date_t::now();
    }

    void NetworkInterfaceASIO::startCommand(const TaskExecutor::CallbackHandle& cbHandle,
                                            const RemoteCommandRequest& request,
                                            const RemoteCommandCompletionFn& onFinish) {
        CommandData cd;
        cd.cbHandle = cbHandle;
        cd.request = request;
        cd.onFinish = onFinish;
        _asyncRunCommand(std::move(cd));
    }

    void NetworkInterfaceASIO::cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) {
        stdx::lock_guard<stdx::mutex> lk(_inProgressMutex);
        for (auto iter = _inProgress.begin(); iter != _inProgress.end(); ++iter) {
            if ((*iter).first->cmd.cbHandle == cbHandle) {
                (*iter).first->cancel();
                return;
            }
        }
    }

} // namespace executor
} // namespace mongo
