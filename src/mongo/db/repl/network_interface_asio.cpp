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

namespace mongo {
    namespace repl {

        using asio::ip::tcp;

        NetworkInterfaceASIO::NetworkInterfaceASIO() :
            _io_service(),
            _timer(_io_service),
            _shutdown(false)
        {
            std::cout << "NETWORK_INTERFACE_ASIO: NetworkInterfaceASIO constructor\n" << std::flush;
        }

        NetworkInterfaceASIO::~NetworkInterfaceASIO() { }

        std::string NetworkInterfaceASIO::getDiagnosticString() {
            return "nothing to see here, move along";
        }

        // TODO: need different handlers for different types of requests
        void NetworkInterfaceASIO::_messageFromRequest(const ReplicationExecutor::RemoteCommandRequest& request,
                                                       Message& toSend) {
            std::cout << "NETWORK_INTERFACE_ASIO: _messageFromRequest\n" << std::flush;
            BSONObj query = request.cmdObj;
            verify(query.isValid());

            BufBuilder b;
            b.appendNum(0); // opts, check default
            b.appendStr(request.dbname + ".$cmd");
            b.appendNum(0); // toSkip
            b.appendNum(0); // toReturn, don't care about responses
            query.appendSelfToBufBuilder(b);

            // wrap up the message object, add headers etc.
            toSend.setData(dbQuery, b.buf(), b.len()); // must b outlive toSend?
            toSend.header().setId(nextMessageId());
            toSend.header().setResponseTo(0);
        }

        void NetworkInterfaceASIO::_asyncSendSimpleMessage(const boost::shared_ptr<AsyncOp> op,
                                                           const asio::const_buffer& buf) {
            std::cout << "NETWORK_INTERFACE_ASIO: sending simple message\n" << std::flush;
            asio::async_write(
                op->_sock, asio::buffer(buf),
                [this, op](std::error_code ec, std::size_t bytes) {
                    if (ec) {
                        // TODO handle legacy command errors and retry
                        std::cout << "NETWORK_INTERFACE_ASIO: a network error occurred\n" << std::flush;
                        _networkErrorCallback(op, ec);
                    } else {
                        std::cout << "NETWORK_INTERFACE_ASIO: sent " << bytes << " bytes\n" << std::flush;
                        _completedWriteCallback(op);
                    }
                    std::cout << "NETWORK_INTERFACE_ASIO: done sending simple message\n" << std::flush;
                });
        }

        void NetworkInterfaceASIO::_asyncSendComplicatedMessage(
               const boost::shared_ptr<AsyncOp> op,
               std::vector<std::pair<char*, int>> data,
               std::vector<std::pair<char*, int>>::const_iterator i) {
            std::cout << "NETWORK_INTERFACE_ASIO: sending complicated message\n" << std::flush;
            // if we are done, call callback from here
            if (i == data.end()) {
                _completedWriteCallback(op);
            }

            // otherwise, send another buffer
            asio::const_buffer buf(i->first, i->second); // data, length
            i++;

            asio::async_write(op->_sock, asio::buffer(buf),
                              [this, data, i, op]
                              (std::error_code ec, std::size_t) {
                   if (ec) {
                       std::cout << "NETWORK_INTERFACE_ASIO: error sending complicated message\n" << std::flush;
                       _networkErrorCallback(op, ec);
                   } else {
                       std::cout << "NETWORK_INTERFACE_ASIO: calling send again\n";
                       _asyncSendComplicatedMessage(op, data, i);
                   }
               });
        }

        void NetworkInterfaceASIO::_completedWriteCallback(const boost::shared_ptr<AsyncOp> op) {
            std::cout << "NETWORK_INTERFACE_ASIO: completed the write\n" << std::flush;
            const Date_t end = now();

            // TODO make this real
            BSONObj output;

            // call the request object's callback fn
            ResponseStatus status(Response(output, Milliseconds(end - op->_start)));
            op->_cmd.onFinish(status);
            std::cout << "NETWORK_INTERFACE_ASIO: successfully called onFinish\n" << std::flush;
        }

        void NetworkInterfaceASIO::_networkErrorCallback(const boost::shared_ptr<AsyncOp> op,
                                                         std::error_code ec) {
            std::cout << "NETWORK_INTERFACE_ASIO: in error callback handler\n" << std::flush;
        }

        void NetworkInterfaceASIO::_asyncRunCmd(const CommandData&& cmd) {
            ReplicationExecutor::RemoteCommandRequest request = cmd.request;
            std::cout << "NETWORK_INTERFACE_ASIO: asyncRunCommand\n" << std::flush;

            boost::shared_ptr<AsyncOp> op(boost::make_shared<AsyncOp>(std::move(cmd), now(), &_io_service));

            // translate request into Message
            Message m;
            _messageFromRequest(op->_cmd.request, m);

            std::cout << "NETWORK_INTERFACE_ASIO: starting async send\n" << std::flush;

            // async send
            if (m.empty()) {
                // call into callback directly
                std::cout << "NETWORK_INTERFACE_ASIO: empty message, call callback\n" << std::flush;
                _completedWriteCallback(op);
            } else if (m._buf!= 0) {
                // simple send
                std::cout << "NETWORK_INTERFACE_ASIO: it's a simple message\n" << std::flush;
                asio::const_buffer buf(m._buf, MsgData::ConstView(m._buf).getLen());
                _asyncSendSimpleMessage(op, buf);
            } else {
                std::cout << "NETWORK_INTERFACE_ASIO: it's a complicated message\n" << std::flush;
                // complex send
                std::vector<std::pair<char *, int>> data = m._data;
                std::vector<std::pair<char *, int>>::const_iterator i = data.begin();
                _asyncSendComplicatedMessage(op, data, i);
            }
        }

        void NetworkInterfaceASIO::_runCommand(const CommandData&& cmd) {
            std::cout << "NETWORK_INTERFACE_ASIO: running command " << cmd.request.cmdObj
                      << " against database " << cmd.request.dbname
                      << " across network to " << cmd.request.target.toString() << "\n" << std::flush;
            _asyncRunCmd(std::move(cmd));
        }

        void NetworkInterfaceASIO::startup() {
            std::cout << "NETWORK_INTERFACE_ASIO: Network Interface starting up...\n" << std::flush;

            _serviceRunner = std::thread([this]() {
                    std::cout << "NETWORK_INTERFACE_ASIO: running io_service\n" << std::flush;
                    asio::io_service::work work(_io_service);
                    _io_service.run();
                    std::cout << "service.run() returned\n" << std::flush;
                });

            std::cout << "NETWORK_INTERFACE_ASIO: done starting up\n" << std::flush;
            return;
        }

        void NetworkInterfaceASIO::shutdown() {
            std::cout << "NETWORK_INTERFACE_ASIO: shutting down\n" << std::flush;
            _shutdown = true;
            _io_service.stop();
            _serviceRunner.join();
            return;
        }

        void NetworkInterfaceASIO::waitForWork() {
            std::cout << "NETWORK_INTERFACE_ASIO: waiting for work...\n" << std::flush;
            return;
        }

        void NetworkInterfaceASIO::waitForWorkUntil(Date_t when) {
            std::cout << "NETWORK_INTERFACE_ASIO: waiting for work until " << when << "...\n" << std::flush;
            return;
        }

        void NetworkInterfaceASIO::signalWorkAvailable() {
            std::cout << "NETWORK_INTERFACE_ASIO: work is available, signaling\n" << std::flush;
            return;
        }

        Date_t NetworkInterfaceASIO::now() {
            return curTimeMillis64();
        }

        void NetworkInterfaceASIO::startCommand(
                const ReplicationExecutor::CallbackHandle& cbHandle,
                const ReplicationExecutor::RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish) {

            std::cout << "NETWORK_INTERFACE_ASIO: beginning command\n" << std::flush;
            LOG(2) << "Scheduling " << request.cmdObj.firstElementFieldName() << " to " <<
                request.target;

            CommandData cd = CommandData();
            cd.cbHandle = cbHandle;
            cd.request = request;
            cd.onFinish = onFinish;
            _runCommand(std::move(cd));
        }

        void NetworkInterfaceASIO::cancelCommand(const ReplicationExecutor::CallbackHandle& cbHandle) {
            std::cout << "NETWORK_INTERFACE_ASIO: canceling command\n" << std::flush;
            return;
        }

        void NetworkInterfaceASIO::runCallbackWithGlobalExclusiveLock(
            const stdx::function<void (OperationContext*)>& callback) {
            std::cout << "NETWORK_INTERFACE_ASIO: running callback with the global exclusive locl\n" << std::flush;
            return;
        }

        OperationContext* NetworkInterfaceASIO::createOperationContext() {
            Client::initThreadIfNotAlready();
            return new OperationContextImpl();
        }
    } // namespace repl
} // namespace mongo
