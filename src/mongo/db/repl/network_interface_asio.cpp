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
#include "mongo/util/time_support.h"

namespace mongo {
    namespace repl {

        NetworkInterfaceASIO::NetworkInterfaceASIO() : _shutdown(false) { }

        NetworkInterfaceASIO::~NetworkInterfaceASIO() { }

        std::string NetworkInterfaceASIO::getDiagnosticString() {
            return "nothing to see here, move along";
        }

        void NetworkInterfaceASIO::_sendMessageToHostAndPort(const Message& m, const HostAndPort& addr) {
            // make a messaging port
            // send message on port
            // don't care about response.
        }

        void NetworkInterfaceASIO::_sendRequestToHostAndPort(
            const ReplicationExecutor::RemoteCommandRequest& request,
            const HostAndPort& addr) {

            // RemoteCommandRequest -> BSONObj
            BSONObj query = request.cmdObj;

            // BSONObj -> Message
            BufBuilder b;
            b.appendNum(0); // opts, check default
            b.appendStr(request.dbname + ".$cmd");
            b.appendNum(0); // toSkip
            b.appendNum(0); // toReturn, don't care about responses
            query.appendSelfToBufBuilder(b);

            // wrap up the message object, add headers etc.
            Message toSend;
            toSend.setData(dbQuery, b.buf(), b.len()); // may need to change this type? Where from, Message?
            toSend.header().setId(nextMessageId()); // this method lives in Message
            toSend.header().setResponseTo(0);

            // send
            _sendMessageToHostAndPort(toSend, addr);
        }

        void NetworkInterfaceASIO::_sendRequest(const ReplicationExecutor::RemoteCommandRequest& request) {
            _sendRequestToHostAndPort(request, request.target);
        }

        ResponseStatus NetworkInterfaceASIO::_runCommand(
            const ReplicationExecutor::RemoteCommandRequest& request) {

            try {
                BSONObj info;
                const Date_t requestStartDate = now();

                // TODO: tunnel info through
                _sendRequest(request);

                const Date_t requestFinishDate = now();
                //conn.done(requestFinishDate);
                return ResponseStatus(Response(info, Milliseconds(requestFinishDate - requestStartDate)));
            }
            catch (const DBException& ex) {
                return ResponseStatus(ex.toStatus());
            }
            catch (const std::exception& ex) {
                return ResponseStatus(
                                      ErrorCodes::UnknownError,
                                      mongoutils::str::stream() <<
                                      "Sending command " << request.cmdObj << " on database " << request.dbname <<
                                      " over network to " << request.target.toString() << " received exception " <<
                                      ex.what());
            }
        }

        void NetworkInterfaceASIO::_listen() {
            while (!_shutdown) {
                if (_pending.empty()) {
                    sleep(1);
                    continue;
                }

                CommandData task = _pending.front();
                _pending.pop_front();

                ResponseStatus result = _runCommand(task.request);
                LOG(2) << "Network status of sending " << task.request.cmdObj.firstElementFieldName() <<
                    " to " << task.request.target << " was " << result.getStatus();
                task.onFinish(result);
            }
        }

        void NetworkInterfaceASIO::_launchThread(NetworkInterfaceASIO* net, const std::string& threadName) {
            LOG(1) << "thread starting";
            net->_listen();
            LOG(1) << "thread ending";
        }

        void NetworkInterfaceASIO::startup() {
            const std::string threadName("ReplExecASIO_listen");
            try {
                _workerThread = boost::make_shared<boost::thread>(stdx::bind(&NetworkInterfaceASIO::_launchThread,
                                                                             this,
                                                                             "aaaaah"));
            }
            catch (const std::exception& ex) {
                LOG(1) << "Failed to start " << threadName << "; caught exception: " << ex.what();
            }

            return;
        }

        void NetworkInterfaceASIO::shutdown() {
            _shutdown = true;
            _workerThread->boost::thread::join();
        }

        void NetworkInterfaceASIO::waitForWork() {
            return;
        }

        void NetworkInterfaceASIO::waitForWorkUntil(Date_t when) {
            return;
        }

        void NetworkInterfaceASIO::signalWorkAvailable() {
            return;
        }

        Date_t NetworkInterfaceASIO::now() {
            return curTimeMillis64();
        }

        void NetworkInterfaceASIO::startCommand(
                const ReplicationExecutor::CallbackHandle& cbHandle,
                const ReplicationExecutor::RemoteCommandRequest& request,
                const RemoteCommandCompletionFn& onFinish) {
            LOG(2) << "Scheduling " << request.cmdObj.firstElementFieldName() << " to " <<
                request.target;

            _pending.push_back(CommandData());

            CommandData& cd = _pending.back();
            cd.cbHandle = cbHandle;
            cd.request = request;
            cd.onFinish = onFinish;
        }

        void NetworkInterfaceASIO::cancelCommand(const ReplicationExecutor::CallbackHandle& cbHandle) {
            return;
        }

        void NetworkInterfaceASIO::runCallbackWithGlobalExclusiveLock(
            const stdx::function<void (OperationContext*)>& callback) {

        }

    } // namespace repl
} // namespace mongo
