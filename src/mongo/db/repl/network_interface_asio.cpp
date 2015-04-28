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

#include <boost/make_shared.hpp>
#include <boost/thread.hpp>

#include "mongo/bson/bsonobj.h"
#include "mongo/util/log.h"
#include "mongo/stdx/memory.h"

namespace mongo {
    namespace repl {

        using asio::ip::tcp;

        NetworkInterfaceASIO::NetworkInterfaceASIO() :
            _io_service(),
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

            BufBuilder b;
            b.appendNum(0); // opts, check default
            b.appendStr(request.dbname + ".$cmd");
            b.appendNum(0); // toSkip
            b.appendNum(0); // toReturn, don't care about responses
            query.appendSelfToBufBuilder(b);

            std::cout << "NETWORK_INTERFACE_ASIO: making bson obj into message\n" << std::flush;

            // wrap up the message object, add headers etc.
            toSend.setData(dbQuery, b.buf(), b.len()); // must b outlive toSend?
            toSend.header().setId(nextMessageId());
            toSend.header().setResponseTo(0);

            std::cout << "NETWORK_INTERFACE_ASIO: done!\n" << std::flush;
        }

        void NetworkInterfaceASIO::_asyncSendSimpleMessage(const std::unique_ptr<AsyncOp>& op,
                                                           const asio::const_buffer& buf) {
            std::cout << "address of op:" << op.get() << std::flush;
            std::cout << "NETWORK_INTERFACE_ASIO: sending simple message\n" << std::flush;
            asio::async_write(
                              op->_sock, asio::buffer(buf),
                              [this, &op](std::error_code ec, std::size_t /*length*/) {
                                  std::cout << "address of op:" << &op << std::flush;
                                  std::cout << "NETWORK_INTERFACE_ASIO: async_write\n" << std::flush;
                                  if (ec) {
                                      // TODO handle legacy command errors and retry
                                      std::cout << "NETWORK_INTERFACE_ASIO: a network error occurred :(\n" << std::flush;
                                      _networkErrorCallback(op, ec);
                                  } else {
                                      std::cout << "NETWORK_INTERFACE_ASIO: send complete\n" << std::flush;
                                      _completedWriteCallback(op);
                                  }
                              });
        }

        void NetworkInterfaceASIO::_asyncSendComplicatedMessage(
               const std::unique_ptr<AsyncOp>& op,
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
                              [this, data, i, &op]
                              (std::error_code ec, std::size_t) {
                   if (ec) {
                       std::cout << "NETWORK_INTERFACE_ASIO: a network error sending complicated message\n" << std::flush;
                       _networkErrorCallback(op, ec);
                   } else {
                       _asyncSendComplicatedMessage(op, data, i);
                   }
               });
        }

        void NetworkInterfaceASIO::_completedWriteCallback(const std::unique_ptr<AsyncOp>& op) {
            std::cout << "NETWORK_INTERFACE_ASIO: completed the write\n" << std::flush;
            const Date_t end = now();

            // TODO make this real
            BSONObj output;

            // call the request object's callback fn
            ResponseStatus status(Response(output, Milliseconds(end - op->_start)));
            op->_cmd.onFinish(status);
        }

        void NetworkInterfaceASIO::_networkErrorCallback(const std::unique_ptr<AsyncOp>& op,
                                                         std::error_code ec) {
            std::cout << "NETWORK_INTERFACE_ASIO: in error callback handler\n" << std::flush;
        }

        void NetworkInterfaceASIO::_asyncRunCmd(const CommandData&& cmd) {
            ReplicationExecutor::RemoteCommandRequest request = cmd.request;
            std::cout << "NETWORK_INTERFACE_ASIO: asyncRuNCommand\n" << std::flush;

            // get a socket to use for this operation, connected to HostAndPort
            // but put it on the heap
            //tcp::socket sock(_io_service);

            std::unique_ptr<AsyncOp> op = stdx::make_unique<AsyncOp>(std::move(cmd), now(),
                                                                     &_io_service);

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

        void NetworkInterfaceASIO::_listen() {
            do {
                // run at least once
                std::cout << "NETWORK_INTERFACE_ASIO: listening...\n" << std::flush;
                if (_pending.empty()) {
                    sleep(10);
                    continue;
                }

                std::cout << "NETWORK_INTERFACE_ASIO: handling task\n" << std::flush;
                CommandData task = _pending.front();
                _pending.pop_front();

                _runCommand(std::move(task));
            } while (!_shutdown);
        }

        void NetworkInterfaceASIO::_launchThread(NetworkInterfaceASIO* net, const std::string& threadName) {
            std::cout << "NETWORK_INTERFACE_ASIO: launching thread " << threadName << "\n" << std::flush;
            LOG(1) << "thread starting";
            net->_listen();
            LOG(1) << "thread ending";
            std::cout << "NETWORK_INTERFACE_ASIO: shutting down thread " << threadName << "\n" << std::flush;
        }

        // TODO: function that launches the io_service

        // ditch the queue and the condition variable and stuff, let ASIO handle that.
        // replication executor should call directly into thing that launches async_write
        // no threads to wait on thing.

        // could store the AOs in a std::set (protect with a lock), then use it in callback, then delete

        void NetworkInterfaceASIO::startup() {
            std::cout << "NETWORK_INTERFACE_ASIO: Network Interface starting up...\n" << std::flush;
            //const std::string threadName("ReplExecASIO_listen");
            // try {
            //     _workerThread = boost::make_shared<boost::thread>(stdx::bind(&NetworkInterfaceASIO::_launchThread,
            //                                                                  this,
            //                                                                  threadName));
            // }
            // catch (const std::exception& ex) {
            //     LOG(1) << "Failed to start " << threadName << "; caught exception: " << ex.what();
            // }

            // TODO: launch io service
            _serviceRunner = boost::thread([this]() {
                    _io_service.run();
                });

            std::cout << "NETWORK_INTERFACE_ASIO: done starting up\n" << std::flush;
            return;
        }

        void NetworkInterfaceASIO::shutdown() {
            std::cout << "NETWORK_INTERFACE_ASIO: shutting down\n" << std::flush;

            _shutdown = true;
            //_workerThread->boost::thread::join();

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

            _pending.push_back(CommandData());

            CommandData& cd = _pending.back();
            cd.cbHandle = cbHandle;
            cd.request = request;
            cd.onFinish = onFinish;
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

    } // namespace repl
} // namespace mongo
