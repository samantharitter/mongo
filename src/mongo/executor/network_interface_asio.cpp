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

#include <asio/basic_deadline_timer.hpp>
#include <chrono>

#include "mongo/bson/bsonobj.h"
#include "mongo/db/client.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/executor/network_interface_asio.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/log.h"
#include "mongo/util/net/hostandport.h"

namespace mongo {
namespace executor {

    using asio::ip::tcp;
    using RemoteCommandCompletionFn =
        stdx::function<void (const TaskExecutor::ResponseStatus&)>;

    /**
     * Information describing an in-flight command.
     */
    struct NetworkInterfaceASIO::CommandData {
        TaskExecutor::CallbackHandle cbHandle;
        RemoteCommandRequest request;
        RemoteCommandCompletionFn onFinish;
    };

    /**
     * A helper type for async networking operations
     */
    struct NetworkInterfaceASIO::AsyncOp {
        AsyncOp(CommandData&& cmdObj,
                Date_t now,
                asio::io_service* service,
                ConnectionPool* pool) :
            canceled(false),
            start(now),
            cmd(std::move(cmdObj)),
            _service(service),
            _pool(pool)
        {}

        // throws if any issues with connection occur
        void connect(Date_t now) {
            _conn = stdx::make_unique<ConnectionPool::ConnectionPtr>(_pool,
                                                                     cmd.request.target,
                                                                     now,
                                                                     Milliseconds(10000));
            _sock = stdx::make_unique<tcp::socket>(*_service,
                                                   asio::ip::tcp::v6(),
                                                   _conn->get()->port().psock->rawFD());
        }

        bool disconnect(Date_t now) {
            _conn->done(now, true);
            toSend.reset();
            toRecv.reset();
            return true;
        }

        tcp::socket* sock() {
            return _sock.get();
        }

        std::atomic_bool canceled;

        Message toSend;
        Message toRecv;
        MSGHEADER::Value header;

        // this is owned by toRecv, freed by toRecv.reset()
        char* toRecvBuf;

        BSONObj output;
        const Date_t start;
        CommandData cmd;

    private:
        asio::io_service* const _service;
        ConnectionPool* const _pool;

        std::unique_ptr<ConnectionPool::ConnectionPtr> _conn;
        std::unique_ptr<tcp::socket> _sock;
    };

    NetworkInterfaceASIO::NetworkInterfaceASIO() :
        _io_service(),
        _timer(_io_service),
        _shutdown(false),
        _isExecutorRunnable(false)
    {
        _connPool = stdx::make_unique<ConnectionPool>(kMessagingPortKeepOpen);
    }

    std::string NetworkInterfaceASIO::getDiagnosticString() {
        boost::lock_guard<boost::mutex> lk(_mutex);
        str::stream output;
        output << "NetworkInterfaceASIO";
        output << " inShutdown: " << _shutdown.load();
        output << " inProgress ops: " << _inProgress.size();
        output << " execRunnable: " << _isExecutorRunnable;
        return output;
    }

    // TODO: we'll need different handlers for different types of requests,
    // currently this only handles queries, sync up with Adam.
    void NetworkInterfaceASIO::_messageFromRequest(const RemoteCommandRequest& request,
                                                       Message* toSend) {
        BSONObj query = request.cmdObj;
        invariant(query.isValid());

        BufBuilder b;
        b.appendNum(0); // opts
        b.appendStr(request.dbname + ".$cmd");
        b.appendNum(0); // toSkip
        b.appendNum(1); // toReturn, don't care about responses
        query.appendSelfToBufBuilder(b);

        // wrap up the message object, add headers etc.
        toSend->setData(dbQuery, b.buf(), b.len()); // must b outlive toSend?
        toSend->header().setId(nextMessageId());
        toSend->header().setResponseTo(0);
    }

    void NetworkInterfaceASIO::_asyncSendSimpleMessage(const SharedAsyncOp& op,
                                                       const asio::const_buffer& buf) {
        asio::async_write(*(op->sock()), asio::buffer(buf),
            [this, op](std::error_code ec, std::size_t bytes) {
                 if (ec)
                     return _networkErrorCallback(op, ec);
                 _receiveResponse(op);
            });
    }

    void NetworkInterfaceASIO::_asyncSendVectorMessage(
        const SharedAsyncOp& op,
        const std::vector<std::pair<char*, int>>& data,
        std::vector<std::pair<char*, int>>::const_iterator i) {
        // if we are done, call callback from here
        if (i == data.end()) {
            return _completedWriteCallback(op);
        }

        // otherwise, send another buffer
        asio::const_buffer buf(i->first, i->second); // data, length
        i++;

        asio::async_write(*(op->sock()), asio::buffer(buf),
            [this, &data, i, op](std::error_code ec, std::size_t) {
                if (ec) {
                    LOG(3) << "error sending vector message\n";
                    _networkErrorCallback(op, ec);
                } else {
                    _asyncSendVectorMessage(op, data, i);
                }
            });
        }

    void NetworkInterfaceASIO::_receiveResponse(const SharedAsyncOp& op) {
        _recvMessageHeader(op);
    }

    void NetworkInterfaceASIO::_validateMessageHeader(const SharedAsyncOp& op) {
        // TODO: this error code should be more meaningful.
        std::error_code ec;

        // validate message length
        int len = op->header.constView().getMessageLength();
        if (len == 542393671) {
            LOG(3) << "attempt to access MongoDB over HTTP on the native driver port.\n";
            _networkErrorCallback(op, ec);
        } else if (len == -1) {
            // TODO: an endian check is run after the client connects, we should
            // set that we've received the client's handshake
            LOG(3) << "Endian check received from client\n";
        } else if (static_cast<size_t>(len) < sizeof(MSGHEADER::Value) ||
                   static_cast<size_t>(len) > MaxMessageSizeBytes) {
            LOG(0) << "recv(): message len " << len << " is invalid. "
                   << "Min " << sizeof(MSGHEADER::Value) << " Max: " << MaxMessageSizeBytes;
            _networkErrorCallback(op, ec);
        }

        // validate response id
        size_t expectedId = op->toSend.header().getId();
        size_t actualId = op->header.constView().getResponseTo();
        if (actualId != expectedId) {
            LOG(3) << "got wrong response:"
                   << "\n   expected response id: " << expectedId
                   << "\n   instead got response id: " << actualId << "\n";
            _networkErrorCallback(op, ec);
        }
        _recvMessageBody(op);
    }

    void NetworkInterfaceASIO::_recvMessageBody(const SharedAsyncOp& op) {
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
                    _networkErrorCallback(op, ec);
                } else {
                    op->toRecv.setData((char *)md_view.view2ptr(), true);
                    _completedWriteCallback(op);
                }
        });
    }

    void NetworkInterfaceASIO::_recvMessageHeader(const SharedAsyncOp& op) {
        asio::async_read(*(op->sock()),
                         asio::buffer(reinterpret_cast<char *>(&op->header),
                                      sizeof(MSGHEADER::Value)),
                         [this, op](asio::error_code ec, size_t bytes) {
                             if (ec) {
                                 LOG(3) << "error receiving header\n";
                                 _networkErrorCallback(op, ec);
                             } else {
                                 _validateMessageHeader(op);
                             }
                         });
        }

    void NetworkInterfaceASIO::_completedWriteCallback(const SharedAsyncOp& op) {
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

    void NetworkInterfaceASIO::_networkErrorCallback(const SharedAsyncOp& op,
                                                     const std::error_code& ec) {
        if (op->toRecv._buf) {
            QueryResult::View qr = op->toRecv.singleData().view2ptr();
            LOG(3) << "networking error receiving part of message "
                   << op->toRecv.header().getId() << "\n";
            op->output = BSONObj(qr.data());
        } else if (op->toRecv.empty()) {
            LOG(3) << "networking error occurred, toRecv is empty\n";
            op->output = BSONObj();
        } else {
            LOG(3) << "toRecv is a non-empty complicated message\n";
            // TODO: handle this better.
            op->output = BSONObj();
        }

        _completeOperation(op);
    }

    void NetworkInterfaceASIO::_completeOperation(const SharedAsyncOp& op) {
        const Date_t end = now();
        TaskExecutor::ResponseStatus status(Response(op->output, Milliseconds(end - op->start)));
        if (!op->canceled.load()) {
            op->cmd.onFinish(status);
        }

        op->disconnect(end);
        _inProgress.remove(op.get());
        signalWorkAvailable();
    }

    void NetworkInterfaceASIO::_asyncRunCommand(CommandData&& cmd) {
        LOG(3) << "running command " << cmd.request.cmdObj
               << " against database " << cmd.request.dbname
               << " across network to " << cmd.request.target.toString() << "\n";

        // auth in a separate thread to avoid blocking the rest of the system
        std::thread t([this, &cmd]() {

                if (_shutdown.load()) {
                    return;
                }

                RemoteCommandRequest request = cmd.request;
                SharedAsyncOp op(std::make_shared<AsyncOp>(std::move(cmd),
                                                           now(),
                                                           &_io_service,
                                                           _connPool.get()));

                try {
                    op->connect(now());
                } catch (...) {
                    LOG(3) << "connect() failed, posting mock completion\n";
                    if (_shutdown.load()) {
                        return;
                    }

                    TaskExecutor::ResponseStatus status(exceptionToStatus());
                    asio::post(_io_service, [this, op, status]() {
                            if (!op->canceled.load()) {
                                op->cmd.onFinish(status);
                                _inProgress.remove(op.get());
                            }
                            // TODO: check all calls to this, deadlock-prone, takes lock
                            signalWorkAvailable();
                        });
                    return;
                }

                // send control back to main thread(pool)
                asio::post(_io_service, [this, op]() {
                        _messageFromRequest(op->cmd.request, &op->toSend);
                        _inProgress.push_back(op.get());
                        if (op->toSend.empty()) {
                            _completedWriteCallback(op);
                        } else if (op->toSend._buf != 0) {
                            asio::const_buffer buf(op->toSend._buf, op->toSend.size());
                            _asyncSendSimpleMessage(op, buf);
                        } else {
                            auto iter = op->toSend._data.begin();
                            _asyncSendVectorMessage(op, op->toSend._data, iter);
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
        return;
    }

    void NetworkInterfaceASIO::shutdown() {
        _shutdown.store(true);
        _io_service.stop();
        _serviceRunner.join();
        return;
    }

    void NetworkInterfaceASIO::signalWorkAvailable() {
        boost::lock_guard<boost::mutex> lk(_mutex);
        _signalWorkAvailable_inlock();
    }

    void NetworkInterfaceASIO::_signalWorkAvailable_inlock() {
        if (!_isExecutorRunnable) {
            _isExecutorRunnable = true;
            _isExecutorRunnableCondition.notify_one();
        }
    }

    void NetworkInterfaceASIO::waitForWork() {
        boost::unique_lock<boost::mutex> lk(_mutex);
        while (!_isExecutorRunnable) {
            _isExecutorRunnableCondition.wait(lk);
        }
        _isExecutorRunnable = false;
    }

    void NetworkInterfaceASIO::waitForWorkUntil(Date_t when) {
        boost::unique_lock<boost::mutex> lk(_mutex);
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
        CommandData cd = CommandData();
        cd.cbHandle = cbHandle;
        cd.request = request;
        cd.onFinish = onFinish;
        _asyncRunCommand(std::move(cd));
    }

    void NetworkInterfaceASIO::cancelCommand(const TaskExecutor::CallbackHandle& cbHandle) {
        AsyncOpList::iterator iter;
        for (iter = _inProgress.begin(); iter != _inProgress.end(); ++iter) {
            if (*iter != nullptr) {
                if ((*iter)->cmd.cbHandle == cbHandle) {
                    break;
                }
            }
        }

        if (iter == _inProgress.end() || *iter == nullptr) {
            return;
        }

        (*iter)->canceled.store(true);

        // do we still want to do this?
        signalWorkAvailable();
        return;
    }

} // namespace executor
} // namespace mongo
