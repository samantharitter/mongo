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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kReplication

#include "mongo/platform/basic.h"

#include "mongo/db/repl/network_interface_asio.h"

#include <chrono>

#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

#include "asio/basic_deadline_timer.hpp"

#include "mongo/bson/bsonobj.h"
#include "mongo/util/log.h"
#include "mongo/stdx/memory.h"
#include "mongo/db/client.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/dbmessage.h"

namespace mongo {
    namespace repl {

        using asio::ip::tcp;

        NetworkInterfaceASIO::NetworkInterfaceASIO() :
            _io_service(),
            _timer(_io_service),
            _shutdown(false),
            _isExecutorRunnable(false)
        {
            std::cout << "NETWORK_INTERFACE_ASIO: NetworkInterfaceASIO constructor\n";
            _connPool.reset(new ConnectionPool(kMessagingPortKeepOpen));
        }

        NetworkInterfaceASIO::~NetworkInterfaceASIO() { }

        std::string NetworkInterfaceASIO::getDiagnosticString() {
            return "nothing to see here, move along";
        }

        // TODO: need different handlers for different types of requests
        void NetworkInterfaceASIO::_messageFromRequest(const RemoteCommandRequest& request,
                                                       Message& toSend) {
            BSONObj query = request.cmdObj;
            verify(query.isValid());

            BufBuilder b;
            b.appendNum(0); // opts, check default
            b.appendStr(request.dbname + ".$cmd");
            b.appendNum(0); // toSkip
            b.appendNum(1); // toReturn, don't care about responses
            query.appendSelfToBufBuilder(b);

            // wrap up the message object, add headers etc.
            toSend.setData(dbQuery, b.buf(), b.len()); // must b outlive toSend?
            toSend.header().setId(nextMessageId());
            toSend.header().setResponseTo(0);

            std::cout << "NETWORK_INTERFACE_ASIO: sending _messageFromRequest with id: " << toSend.header().getId()
                      << "\n";
        }

        void NetworkInterfaceASIO::_asyncSendSimpleMessage(sharedAsyncOp op,
                                                           const asio::const_buffer& buf) {
            asio::async_write(*(op->sock()), asio::buffer(buf),
                [this, op](std::error_code ec, std::size_t bytes) {
                    if (ec) {
                        // TODO handle legacy command errors and retry
                        std::cout << "NETWORK_INTERFACE_ASIO: a network error occurred " << op.get() << "\n";
                        _networkErrorCallback(op, ec);
                    } else {
                        //std::cout << "NETWORK_INTERFACE_ASIO: sent " << bytes << " bytes " << op.get() << "\n";
                        _receiveResponse(op);
                    }
                });
        }

        void NetworkInterfaceASIO::_asyncSendComplicatedMessage(
               sharedAsyncOp op,
               std::vector<std::pair<char*, int>> data,
               std::vector<std::pair<char*, int>>::const_iterator i) {
            std::cout << "NETWORK_INTERFACE_ASIO: sending complicated message\n";
            // if we are done, call callback from here
            if (i == data.end()) {
                _completedWriteCallback(op);
            }

            // otherwise, send another buffer
            asio::const_buffer buf(i->first, i->second); // data, length
            i++;

            asio::async_write(*(op->sock()), asio::buffer(buf),
                              [this, data, i, op]
                              (std::error_code ec, std::size_t) {
                   if (ec) {
                       std::cout << "NETWORK_INTERFACE_ASIO: error sending complicated message\n";
                       _networkErrorCallback(op, ec);
                   } else {
                       _asyncSendComplicatedMessage(op, data, i);
                   }
               });
        }

        void NetworkInterfaceASIO::_receiveResponse(sharedAsyncOp op) {
            // need to check if we actually need a response
            // see how the impl does it
            _recvMessageHeader(op);
        }

        void NetworkInterfaceASIO::_validateMessageHeader(sharedAsyncOp op) {
            bool valid = true;

            // validate message length
            int len = op->header.constView().getMessageLength();
            if (len == 542393671) {
                std::cout << "NETWORK_INTERFACE_ASIO: no HTTP, sorry\n";
                valid = false;
            } else if (len == -1) {
                std::cout << "NETWORK_INTERFACE_ASIO: endian check?\n";
                valid = false;
            } else if (len == 0) {
                std::cout << "NETWORK_INTERFACE_ASIO: message length 0\n";
                valid = false;
            } else if (static_cast<size_t>(len) < sizeof(MSGHEADER::Value) ||
                       static_cast<size_t>(len) > MaxMessageSizeBytes) {
                std::cout << "NETWORK_INTERFACE_ASIO: invalid message length\n";
                valid = false;
            }

            // validate response id
            if (valid) {
                size_t expectedId = op->toSend.header().getId();
                size_t actualId = op->header.constView().getResponseTo();
                if (actualId != expectedId) {
                    std::cout << "NETWORK_INTERFACE_ASIO: got wrong response:"
                              << "\n   expected response id: " << expectedId
                              << "\n   instead got response id: " << actualId << "\n";
                    valid = false;
                }
            }

            // if we passed, go receive the body
            if (valid) {
                _recvMessageBody(op);
            } else {
                std::cout << "NETWORK_INTERFACE_ASIO: failed header validation\n";
                std::error_code ec;
                _networkErrorCallback(op, ec);
            }
        }

        void NetworkInterfaceASIO::_recvMessageBody(sharedAsyncOp op) {
            std::cout << "NETWORK_INTERFACE_ASIO: receiving message body...\n";

            // len = whole message length, data + header
            int len = op->header.constView().getMessageLength();

            // todo: server code uses crazy padding hack, investigate.
            int z = (len+1023)&0xfffffc00;
            verify(z>=len);
            op->md = reinterpret_cast<char *>(mongoMalloc(z));
            MsgData::View md_view = op->md;

            // copy header data into master buffer
            int headerLen = sizeof(MSGHEADER::Value);
            memcpy(md_view.view2ptr(), &op->header, headerLen);
            int bodyLength = len - headerLen;

            // receive remaining data into md->data
            asio::async_read(*(op->sock()), asio::buffer(md_view.data(), bodyLength),
                             [this, op, md_view](asio::error_code ec, size_t bytes) {
                                 if (ec) {
                                     std::cout << "NETWORK_INTERFACE_ASIO: error receiving message body\n";
                                     _networkErrorCallback(op, ec);
                                 } else {
                                     op->toRecv.setData((char *)md_view.view2ptr(), true);
                                     _completedWriteCallback(op);
                                 }
                             });
        }

        void NetworkInterfaceASIO::_recvMessageHeader(boost::shared_ptr<AsyncOp> op) {
            std::cout << "NETWORK_INTERFACE_ASIO: receiving message header...\n";
            asio::async_read(*(op->sock()), asio::buffer((char *)(&op->header), sizeof(MSGHEADER::Value)),
                             [this, op](asio::error_code ec, size_t bytes) {
                                 if (ec) {
                                     std::cout << "NETWORK_INTERFACE_ASIO: error receiving header\n";
                                     _networkErrorCallback(op, ec);
                                 } else {
                                     _recvMessageBody(op);
                                 }
                             });
        }

        void NetworkInterfaceASIO::_keepAlive(sharedAsyncOp op) {
            _timer.expires_from_now(std::chrono::seconds(1));
            _timer.async_wait([this, op](std::error_code ec) {
                    _keepAlive(op);
                });
        }

        void NetworkInterfaceASIO::_completedWriteCallback(sharedAsyncOp op) {
            std::cout << "NETWORK_INTERFACE_ASIO: _completedWriteCallback()\n";

            if (op->toRecv.empty()) {
                op->output = BSONObj();
                std::cout << "NETWORK_INTERFACE_ASIO: received an empty message\n";
            } else {
                QueryResult::View qr = op->toRecv.singleData().view2ptr();
                // unavoidable copy
                op->output = BSONObj(qr.data()).getOwned();
                std::cout << "NETWORK_INTERFACE_ASIO: received message with id " << op->toRecv.header().getId()
                          << ", it is a response to " << op->toRecv.header().getResponseTo() << "\n";
            }
            _completeOperation(op);
        }

        void NetworkInterfaceASIO::_networkErrorCallback(sharedAsyncOp op,
                                                         std::error_code ec) {
            if (op->toRecv._buf) {
                QueryResult::View qr = op->toRecv.singleData().view2ptr();
                std::cout << "NETWORK_INTERFACE_ASIO: networking error receiving part of message "
                          << op->toRecv.header().getId() << "\n";
                op->output = BSONObj(qr.data());
            } else if (op->toRecv.empty()) {
                std::cout << "NETWORK_INTERFACE_ASIO: networking error occurred, toRecv is empty\n";
                op->output = BSONObj();
            } else {
                std::cout << "NETWORK_INTERFACE_ASIO: toRecv is a non-empty complicated message\n";
                // TODO: handle this better.
                op->output = BSONObj();
            }

            // todo: do this?
            //op->toRecv.reset();
            _completeOperation(op);
        }

        void NetworkInterfaceASIO::_completeOperation(sharedAsyncOp op) {
            // TODO use this
            const Date_t end = now();
            ResponseStatus status(Response(op->output, Milliseconds(end - op->_start)));
            if (!op->_canceled) {
                op->_cmd.onFinish(status);
            }

            op->disconnect(end);
            // TODO dequeue from _inProgress
            _inProgress.remove(op.get());
            signalWorkAvailable();
        }

        void NetworkInterfaceASIO::_asyncRunCmd(const CommandData&& cmd) {
            RemoteCommandRequest request = cmd.request;
            std::cout << "NETWORK_INTERFACE_ASIO: asyncRunCmd()\n";
            sharedAsyncOp op(boost::make_shared<AsyncOp>(std::move(cmd), now(), &_io_service, _connPool.get()));
            bool connected = op->connect(now());
            if (!connected) {
                //op->disconnect(now());
                std::cout << "NETWORK_INTERFACE_ASIO: connect() failed, posting mock completion\n";
                asio::post(_io_service, [this, op]() {
                        // todo: better error status
                        ResponseStatus status(Response(BSONObj(), Milliseconds(0)));
                        op->_cmd.onFinish(status);
                        // todo: check all calls to this, deadlock-prone, takes lock
                        //signalWorkAvailable();
                    });
                return;
            }

            _messageFromRequest(op->_cmd.request, op->toSend);
            _inProgress.push_back(op.get());

            if (op->toSend.empty()) {
                _completedWriteCallback(op);
            } else if (op->toSend._buf != 0) {
                // simple send
                asio::const_buffer buf(op->toSend._buf, op->toSend.size());
                _asyncSendSimpleMessage(op, buf);
            } else {
                // complex send
                std::vector<std::pair<char *, int>> data = op->toSend._data;
                std::vector<std::pair<char *, int>>::const_iterator i = data.begin();
                _asyncSendComplicatedMessage(op, data, i);
            }
        }

        void NetworkInterfaceASIO::_runCommand(const CommandData&& cmd) {
            std::cout << "NETWORK_INTERFACE_ASIO: running command " << cmd.request.cmdObj
                      << " against database " << cmd.request.dbname
                      << " across network to " << cmd.request.target.toString() << "\n";
            _asyncRunCmd(std::move(cmd));
        }

        void NetworkInterfaceASIO::startup() {
            _serviceRunner = std::thread([this]() {
                    std::cout << "NETWORK_INTERFACE_ASIO: running io_service\n";
                    asio::io_service::work work(_io_service);
                    _io_service.run();
                    std::cout << "NETWORK_INTERFACE_ASIO: service.run() returned\n";
                });
            return;
        }

        void NetworkInterfaceASIO::shutdown() {
            _shutdown = true;
            _io_service.stop();
            _serviceRunner.join();
            std::cout << "NETWORK_INTERFACE_ASIO: shut down complete.\n";
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

        void NetworkInterfaceASIO::startCommand(
                const ReplicationExecutor::CallbackHandle& cbHandle,
                const RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish) {
            LOG(2) << "Scheduling " << request.cmdObj.firstElementFieldName() << " to " <<
                request.target;

            CommandData cd = CommandData();
            cd.cbHandle = cbHandle;
            cd.request = request;
            cd.onFinish = onFinish;
            _runCommand(std::move(cd));
        }

        void NetworkInterfaceASIO::cancelCommand(const ReplicationExecutor::CallbackHandle& cbHandle) {
            std::cout << "NETWORK_INTERFACE_ASIO: canceling command\n";
            AsyncOpList::iterator iter;
            for (iter = _inProgress.begin(); iter != _inProgress.end(); ++iter) {
                if (*iter != NULL) {
                    if ((*iter)->_cmd.cbHandle == cbHandle) {
                        break;
                    }
                }
            }

            if (iter == _inProgress.end() || *iter == NULL) {
                std::cout << "NETWORK_INTERFACE_ASIO: no matching op found\n";
                return;
            }

            (*iter)->_canceled = true;

            // do we still want to do this?
            signalWorkAvailable();
            return;
        }

        void NetworkInterfaceASIO::runCallbackWithGlobalExclusiveLock(
            const stdx::function<void (OperationContext*)>& callback) {
            return;
        }

        OperationContext* NetworkInterfaceASIO::createOperationContext() {
            Client::initThreadIfNotAlready();
            return new OperationContextImpl();
        }
    } // namespace repl
} // namespace mongo
