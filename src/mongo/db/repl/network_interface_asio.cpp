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

namespace mongo {
    namespace repl {

        using asio::ip::tcp;

        NetworkInterfaceASIO::NetworkInterfaceASIO() :
            _shutdown(false) { }

        NetworkInterfaceASIO::~NetworkInterfaceASIO() { }

        std::string NetworkInterfaceASIO::getDiagnosticString() {
            return "nothing to see here, move along";
        }

        // TODO: need different handlers for different types of requests
        void NetworkInterfaceASIO::_messageFromRequest(const ReplicationExecutor::RemoteCommandRequest& request,
                                                       Message toSend) {
            BSONObj query = request.cmdObj;

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

        void NetworkInterfaceASIO::_asyncSendSimpleMessage(
            tcp::socket sock,
            const CommandData& cmd,
            const asio::const_buffer& buf,
            const Date_t start) {
            auto self(shared_from_this());

            asio::async_write(
                              sock, asio::buffer(buf),
                              [this, self, cmd, start](std::error_code ec, std::size_t /*length*/) {
                                  std::cout << "async_write\n";
                                  if (ec) {
                                      // TODO handle legacy command errors and retry
                                      std::cout << "a network error occurred :(\n";
                                      _networkErrorCallback(cmd, ec);
                                  } else {
                                      _completedWriteCallback(cmd, start);
                                  }
                              });
        }

        void NetworkInterfaceASIO::_asyncSendComplicatedMessage(
            tcp::socket sock,
            const CommandData& cmd,
            std::vector<std::pair<char*, int>> data,
            std::vector<std::pair<char*, int>>::const_iterator i,
            const Date_t start) {
            auto self(shared_from_this());

            // if we are done, call callback from here
            if (i == data.end()) {
                _completedWriteCallback(cmd, start);
            }

            // otherwise, send another buffer
            asio::const_buffer buf(i->first, i->second); // data, length
            i++;

            asio::async_write(
                              sock, asio::buffer(buf),
                              [this, self, data, i, &cmd, sock, start]
                              (std::error_code ec, std::size_t) {
                   if (ec) {
                       std::cout << "a network error sending complicated message\n";
                       _networkErrorCallback(cmd, ec);
                   } else {
                       _asyncSendComplicatedMessage(sock, cmd, data, i, start);
                   }
               });
        }

        void NetworkInterfaceASIO::_completedWriteCallback(const CommandData& cmd, const Date_t start) {
            std::cout << "completed the write\n";

            const Date_t end = now();

            // TODO make this real
            BSONObj output;

            // call the request object's callback fn
            ResponseStatus status(Response(output, Milliseconds(end - start)));
            cmd.onFinish(status);
        }

        void NetworkInterfaceASIO::_networkErrorCallback(const CommandData& cmd, std::error_code ec) {
            std::cout << "in error callback handler\n";
        }

        void NetworkInterfaceASIO::_asyncRunCmd(const CommandData& cmd) {
            ReplicationExecutor::RemoteCommandRequest request = cmd.request;
            HostAndPort addr = request.target;
            const Date_t start = now();

            // get a socket to use for this operation, connected to HostAndPort
            tcp::socket sock(_io_service);

            // TODO use a non-blocking connect, store network state
            tcp::resolver resolver(_io_service);
            asio::connect(sock, resolver.resolve({addr.host(), std::to_string(addr.port())}));

            // translate request into Message
            Message m;
            _messageFromRequest(request, m);

            // async send
            if (m.empty()) {
                // call into callback directly
                _completedWriteCallback(cmd, start);
            } else if (m._buf!= 0) {
                // simple send
                asio::const_buffer buf(m._buf, MsgData::ConstView(m._buf).getLen());
                _asyncSendSimpleMessage(std::move(sock), cmd, buf, start);
            } else {
                // complex send
                std::vector<std::pair<char *, int>> data = m._data;
                std::vector<std::pair<char *, int>>::const_iterator i = data.begin();
                _asyncSendComplicatedMessage(std::move(sock), cmd, data, i, start);
            }
        }

        void NetworkInterfaceASIO::_runCommand(const CommandData& cmd) {
            std::cout << "running command " << cmd.request.cmdObj
                      << " against database " << cmd.request.dbname
                      << " across network to " << cmd.request.target.toString() << "\n";
            _asyncRunCmd(cmd);
        }

        void NetworkInterfaceASIO::_listen() {
            do {
                // run at least once
                std::cout << "listening...\n";
                if (_pending.empty()) {
                    sleep(1);
                    continue;
                }

                std::cout << "handling task\n";
                CommandData task = _pending.front();
                _pending.pop_front();

                _runCommand(task);
            } while (!_shutdown);
        }

        void NetworkInterfaceASIO::_launchThread(NetworkInterfaceASIO* net, const std::string& threadName) {
            std::cout << "launching thread " << threadName << "\n";
            LOG(1) << "thread starting";
            net->_listen();
            LOG(1) << "thread ending";
            std::cout << "shutting down thread " << threadName << "\n";
        }

        void NetworkInterfaceASIO::startup() {
            std::cout << "Network Interface starting up...\n";
            const std::string threadName("ReplExecASIO_listen");
            try {
                _workerThread = boost::make_shared<boost::thread>(stdx::bind(&NetworkInterfaceASIO::_launchThread,
                                                                             this,
                                                                             threadName));
            }
            catch (const std::exception& ex) {
                LOG(1) << "Failed to start " << threadName << "; caught exception: " << ex.what();
            }
            std::cout << "done starting up\n";
            return;
        }

        void NetworkInterfaceASIO::shutdown() {
            std::cout << "shutting down\n";
            _shutdown = true;
            _workerThread->boost::thread::join();
            return;
        }

        void NetworkInterfaceASIO::waitForWork() {
            std::cout << "waiting for work...\n";
            return;
        }

        void NetworkInterfaceASIO::waitForWorkUntil(Date_t when) {
            std::cout << "waiting for work until " << when << "...\n";
            return;
        }

        void NetworkInterfaceASIO::signalWorkAvailable() {
            std::cout << "work is available, signaling\n";
            return;
        }

        Date_t NetworkInterfaceASIO::now() {
            return curTimeMillis64();
        }

        void NetworkInterfaceASIO::startCommand(
                const ReplicationExecutor::CallbackHandle& cbHandle,
                const ReplicationExecutor::RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish) {
            std::cout << "beginning command\n";
            LOG(2) << "Scheduling " << request.cmdObj.firstElementFieldName() << " to " <<
                request.target;

            _pending.push_back(CommandData());

            CommandData& cd = _pending.back();
            cd.cbHandle = cbHandle;
            cd.request = request;
            cd.onFinish = onFinish;
        }

        void NetworkInterfaceASIO::cancelCommand(const ReplicationExecutor::CallbackHandle& cbHandle) {
            std::cout << "canceling command\n";
            return;
        }

        void NetworkInterfaceASIO::runCallbackWithGlobalExclusiveLock(
            const stdx::function<void (OperationContext*)>& callback) {
            std::cout << "running callback with the global exclusive locl\n";
            return;
        }

    } // namespace repl
} // namespace mongo
