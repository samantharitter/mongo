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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kASIO

#include "mongo/platform/basic.h"

#include "mongo/executor/network_interface_asio.h"

#include "mongo/base/status_with.h"
#include "mongo/db/query/getmore_request.h"
#include "mongo/db/query/lite_parsed_query.h"
#include "mongo/executor/async_stream_interface.h"
#include "mongo/executor/connection_pool_asio.h"
#include "mongo/executor/downconvert_find_and_getmore_commands.h"
#include "mongo/executor/network_interface_asio.h"
#include "mongo/rpc/factory.h"
#include "mongo/rpc/metadata/metadata_hook.h"
#include "mongo/rpc/request_builder_interface.h"
#include "mongo/util/log.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace executor {

using asio::ip::tcp;

namespace {

// Used to generate unique identifiers for AsyncOps for trying, because the same AsyncOp
// might be used to run multiple requests.
AtomicUInt64 kAsyncOpIdCounter(0);

// Metadata listener can be nullptr.
StatusWith<Message> messageFromRequest(const RemoteCommandRequest& request,
                                       rpc::Protocol protocol,
                                       rpc::EgressMetadataHook* metadataHook) {
    BSONObj query = request.cmdObj;
    auto requestBuilder = rpc::makeRequestBuilder(protocol);

    BSONObj maybeAugmented;
    // Handle outgoing request metadata.
    if (metadataHook) {
        BSONObjBuilder augmentedBob;
        augmentedBob.appendElements(request.metadata);

        auto writeStatus = callNoexcept(*metadataHook,
                                        &rpc::EgressMetadataHook::writeRequestMetadata,
                                        request.target,
                                        &augmentedBob);
        if (!writeStatus.isOK()) {
            return writeStatus;
        }

        maybeAugmented = augmentedBob.obj();
    } else {
        maybeAugmented = request.metadata;
    }

    auto toSend = rpc::makeRequestBuilder(protocol)
                      ->setDatabase(request.dbname)
                      .setCommandName(request.cmdObj.firstElementFieldName())
                      .setCommandArgs(request.cmdObj)
                      .setMetadata(maybeAugmented)
                      .done();
    return std::move(toSend);
}

}  // namespace

NetworkInterfaceASIO::AsyncOp::AsyncOp(NetworkInterfaceASIO* const owner,
                                       const TaskExecutor::CallbackHandle& cbHandle,
                                       const RemoteCommandRequest& request,
                                       const RemoteCommandCompletionFn& onFinish,
                                       Date_t now)
    : _owner(owner),
      _cbHandle(cbHandle),
      _request(request),
      _onFinish(onFinish),
      _start(now),
      _resolver(owner->_io_service),
      _id(kAsyncOpIdCounter.addAndFetch(1)),
      _access(std::make_shared<AsyncOp::AccessControl>()),
      _inSetup(true),
      _strand(owner->_io_service) {
    // No need to take lock when we aren't yet constructed.
    _transitionToState_inlock(State::kUninitialized);
}

void NetworkInterfaceASIO::AsyncOp::cancel() {
    LOG(2) << "Canceling operation; original request was: " << request().toString();
    stdx::lock_guard<stdx::mutex> lk(_access->mutex);
    auto access = _access;
    auto generation = access->id;

    // An operation may be in mid-flight when it is canceled, so we cancel any
    // in-progress async ops but do not complete the operation now.

    _strand.post([this, access, generation] {
        stdx::lock_guard<stdx::mutex> lk(access->mutex);
        if (generation == access->id) {
            _transitionToState_inlock(AsyncOp::State::kCanceled);
            if (_connection) {
                _connection->cancel();
            }
        }
    });
}

bool NetworkInterfaceASIO::AsyncOp::canceled() const {
    return _hasSeenState(State::kCanceled);
}

void NetworkInterfaceASIO::AsyncOp::timeOut_inlock() {
    LOG(2) << "Operation timing out; original request was: " << request().toString();
    auto access = _access;
    auto generation = access->id;

    // An operation may be in mid-flight when it times out, so we cancel any
    // in-progress stream operations but do not complete the operation now.

    _strand.post([this, access, generation] {
        stdx::lock_guard<stdx::mutex> lk(access->mutex);
        if (generation == access->id) {
            _transitionToState_inlock(AsyncOp::State::kTimedOut);
            if (_connection) {
                _connection->cancel();
            }
        }
    });
}

bool NetworkInterfaceASIO::AsyncOp::timedOut() const {
    return _hasSeenState(State::kTimedOut);
}

const TaskExecutor::CallbackHandle& NetworkInterfaceASIO::AsyncOp::cbHandle() const {
    return _cbHandle;
}

NetworkInterfaceASIO::AsyncConnection& NetworkInterfaceASIO::AsyncOp::connection() {
    _invariantWithInfo(_connection.is_initialized(), "Connection not yet initialized");
    return *_connection;
}

void NetworkInterfaceASIO::AsyncOp::setConnection(AsyncConnection&& conn) {
    _invariantWithInfo(!_connection.is_initialized(), "Connection already initialized");
    _connection = std::move(conn);
}

Status NetworkInterfaceASIO::AsyncOp::beginCommand(Message&& newCommand,
                                                   AsyncCommand::CommandType type,
                                                   const HostAndPort& target) {
    // NOTE: We operate based on the assumption that AsyncOp's
    // AsyncConnection does not change over its lifetime.
    _invariantWithInfo(_connection.is_initialized(),
                       "Connection should not change over AsyncOp's lifetime");

    // Construct a new AsyncCommand object for each command.
    _command.emplace(_connection.get_ptr(), type, std::move(newCommand), _owner->now(), target);
    return Status::OK();
}

Status NetworkInterfaceASIO::AsyncOp::beginCommand(const RemoteCommandRequest& request,
                                                   rpc::EgressMetadataHook* metadataHook) {
    // Check if we need to downconvert find or getMore commands.
    StringData commandName = request.cmdObj.firstElement().fieldNameStringData();
    const auto isFindCmd = commandName == LiteParsedQuery::kFindCommandName;
    const auto isGetMoreCmd = commandName == GetMoreRequest::kGetMoreCommandName;
    const auto isFindOrGetMoreCmd = isFindCmd || isGetMoreCmd;

    // If we aren't sending a find or getMore, or the server supports OP_COMMAND we don't have
    // to worry about downconversion.
    if (!isFindOrGetMoreCmd || connection().serverProtocols() == rpc::supports::kAll) {
        auto newCommand = messageFromRequest(request, operationProtocol(), metadataHook);
        if (!newCommand.isOK()) {
            return newCommand.getStatus();
        }
        return beginCommand(
            std::move(newCommand.getValue()), AsyncCommand::CommandType::kRPC, request.target);
    } else if (isFindCmd) {
        auto downconvertedFind = downconvertFindCommandRequest(request);
        if (!downconvertedFind.isOK()) {
            return downconvertedFind.getStatus();
        }
        return beginCommand(std::move(downconvertedFind.getValue()),
                            AsyncCommand::CommandType::kDownConvertedFind,
                            request.target);
    } else {
        _invariantWithInfo(isGetMoreCmd, "Expected a GetMore command");
        auto downconvertedGetMore = downconvertGetMoreCommandRequest(request);
        if (!downconvertedGetMore.isOK()) {
            return downconvertedGetMore.getStatus();
        }
        return beginCommand(std::move(downconvertedGetMore.getValue()),
                            AsyncCommand::CommandType::kDownConvertedGetMore,
                            request.target);
    }
}

NetworkInterfaceASIO::AsyncCommand* NetworkInterfaceASIO::AsyncOp::command() {
    _invariantWithInfo(_command.is_initialized(), "Command is not yet initialized");
    return _command.get_ptr();
}

void NetworkInterfaceASIO::AsyncOp::finish(const ResponseStatus& status) {
    // We never hold the access lock when we call finish from NetworkInterfaceASIO.
    _transitionToState(AsyncOp::State::kFinished);

    // Calling the completion handler may invalidate state in this op, so do it last.
    _onFinish(status);
}

const RemoteCommandRequest& NetworkInterfaceASIO::AsyncOp::request() const {
    return _request;
}

void NetworkInterfaceASIO::AsyncOp::startProgress(Date_t startTime) {
    _start = startTime;
    // We never hold the access lock when we call startProgress from NetworkInterfaceASIO.
    _transitionToState(AsyncOp::State::kInProgress);
}

Date_t NetworkInterfaceASIO::AsyncOp::start() const {
    return _start;
}

rpc::Protocol NetworkInterfaceASIO::AsyncOp::operationProtocol() const {
    _invariantWithInfo(_operationProtocol.is_initialized(), "Protocol not yet set");
    return *_operationProtocol;
}

void NetworkInterfaceASIO::AsyncOp::setOperationProtocol(rpc::Protocol proto) {
    _invariantWithInfo(!_operationProtocol.is_initialized(), "Protocol already set");
    _operationProtocol = proto;
}

void NetworkInterfaceASIO::AsyncOp::reset() {
    // We don't reset owner as it never changes
    _cbHandle = {};
    _request = {};
    _onFinish = {};
    _connectionPoolHandle = {};
    // We don't reset _connection as we want to reuse it.
    // Ditto for _operationProtocol.
    _start = {};
    _timeoutAlarm.reset();
    // _id stays the same for the lifetime of this object.
    _command = boost::none;
    // _inSetup should always be false at this point.
    // We never hold the access lock when we call this from NetworkInterfaceASIO.
    clearStateTransitions();
}

void NetworkInterfaceASIO::AsyncOp::clearStateTransitions() {
    _transitionToState(AsyncOp::State::kUninitialized);
}

void NetworkInterfaceASIO::AsyncOp::setOnFinish(RemoteCommandCompletionFn&& onFinish) {
    _onFinish = std::move(onFinish);
}

// Return a string representation of the given state.
std::string NetworkInterfaceASIO::AsyncOp::_stateToString(AsyncOp::State state) const {
    switch (state) {
        case State::kUninitialized:
            return "UNITIALIZED";
        case State::kInProgress:
            return "IN_PROGRESS";
        case State::kTimedOut:
            return "TIMED_OUT";
        case State::kCanceled:
            return "CANCELED";
        case State::kFinished:
            return "DONE";
        case State::kNoState:
            return "---";
        default:
            MONGO_UNREACHABLE;
    }
}

std::string NetworkInterfaceASIO::AsyncOp::_stateString() const {
    str::stream s;
    s << "[ ";

    for (int i = 0; i < kMaxStateTransitions; i++) {
        if (_states[i] == State::kNoState) {
            break;
        }

        if (i != 0) {
            s << ", ";
        }

        s << _stateToString(_states[i]);
    }

    s << " ]";

    return s;
}

// TODO: typedef this as AsyncOpTableRow or something
std::vector<std::string> NetworkInterfaceASIO::AsyncOp::getStringFields() const {
    // We leave a placeholder for an asterisk
    return {"", std::to_string(_id), _stateString(), _start.toString(), _request.toString()};
}

std::string NetworkInterfaceASIO::AsyncOp::toString() const {
    str::stream s;
    for (auto field : getStringFields()) {
        s << field << "\t\t";
    }
    s << "\n";
    return s;
}

bool NetworkInterfaceASIO::AsyncOp::operator==(const AsyncOp& other) const {
    return _id == other._id;
}

bool NetworkInterfaceASIO::AsyncOp::_hasSeenState(AsyncOp::State state) const {
    for (int i = 0; i < kMaxStateTransitions; i++) {
        if (_states[i] == state) {
            return true;
        }
    }
    return false;
}

void NetworkInterfaceASIO::AsyncOp::_transitionToState(AsyncOp::State newState) {
    stdx::lock_guard<stdx::mutex> lk(_access->mutex);
    _transitionToState_inlock(newState);
}

void NetworkInterfaceASIO::AsyncOp::_transitionToState_inlock(AsyncOp::State newState) {
    if (newState == State::kUninitialized) {
        _states[0] = State::kUninitialized;
        for (int i = 1; i < kMaxStateTransitions; i++) {
            _states[i] = State::kNoState;
        }
        return;
    }

    for (int i = 0; i < kMaxStateTransitions; i++) {
        // We can't transition to the same state twice.
        _invariantWithInfo(_states[i] != newState,
                           "Cannot use the same state (" + _stateToString(newState) + ") twice");

        if (_states[i] == State::kNoState) {
            // Perform some validation before transitioning.
            switch (newState) {
                case State::kInProgress:
                    _invariantWithInfo(i == 1,
                                       "kInProgress must come directly after kUninitialized");
                    break;
                case State::kTimedOut:
                case State::kCanceled:
                    _invariantWithInfo(i > 1,
                                       _stateToString(newState) + " must come after kInProgress");
                    _invariantWithInfo(_states[i - 1] != State::kUninitialized,
                                       _stateToString(newState) +
                                           " cannot come after kUninitialized");
                    break;
                case State::kFinished:
                    _invariantWithInfo(i > 0, "kFinished must come after kUninitialized");
                    break;
                default:
                    MONGO_UNREACHABLE;
            }

            // Update state.
            _states[i] = newState;
            return;
        }
    }

    // If we get here, we've already transitioned to the max allowed states, explode.
    MONGO_UNREACHABLE;
}

template <typename Expression>
void NetworkInterfaceASIO::AsyncOp::_invariantWithInfo(Expression&& e, std::string msg) const {
    invariantWithInfo(e,
                      [this, msg]() {
                          return "AsyncOp invariant failure: " + msg + "\n\n\t Operation: " +
                              toString() + "\n\n";
                      });
}

}  // namespace executor
}  // namespace mongo
