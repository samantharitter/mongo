/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/transport/service_entry_point_mongos.h"

#include <vector>

#include "mongo/db/lasterror.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/server_options.h"
#include "mongo/db/service_context.h"
#include "mongo/s/client/shard_connection.h"
#include "mongo/s/commands/request.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/session.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/socket_exception.h"
#include "mongo/util/quick_exit.h"

namespace mongo {

namespace {

static BSONObj buildErrReply(const DBException& ex) {
    BSONObjBuilder errB;
    errB.append("$err", ex.what());
    errB.append("code", ex.getCode());
    if (!ex._shard.empty()) {
        errB.append("shard", ex._shard);
    }
    return errB.obj();
}

}  // namespace

using transport::Session;
using transport::TransportLayer;

ServiceEntryPointMongos::ServiceEntryPointMongos(TransportLayer* tl) : _tl(tl) {}

void ServiceEntryPointMongos::startSession(Session&& session) {
    stdx::thread t(&ServiceEntryPointMongos::_runSession, this, std::move(session));
    t.detach();
}

void ServiceEntryPointMongos::_runSession(Session&& session) {
    Client::initThread("conn", getGlobalServiceContext(), &session);
    setThreadName(std::string(str::stream() << "conn" << session.id()));

    try {
        // Enter the get-Message, handle-Message, send-Message loop
        _sessionLoop(&session);
    } catch (AssertionException& e) {
        log() << "AssertionException handling request, closing client connection: " << e
              << std::endl;
    } catch (SocketException& e) {
        log() << "SocketException handling request, closing client connection: " << e << std::endl;
    } catch (
        const DBException& e) {  // must be right above std::exception to avoid catching subclasses
        log() << "DBException handling request, closing client connection: " << e << std::endl;
    } catch (std::exception& e) {
        error() << "Uncaught std::exception: " << e.what() << ", terminating" << std::endl;
        quickExit(EXIT_UNCAUGHT);
    }

    _tl->end(session);

    if (!serverGlobalParams.quiet) {
        auto conns = _tl->numOpenSessions();
        const char* word = (conns == 1 ? " connection" : " connections");
        log() << "end connection " << session.remote() << " (" << conns << word << " now open)"
              << std::endl;
    }

    // Release any cached egress connections for client back to pool before destroying
    ShardConnection::releaseMyConnections();

    Client::destroy();
}

void ServiceEntryPointMongos::_sessionLoop(Session* session) {
    Message message;
    int64_t counter;

    while (true) {
        if (inShutdown()) {
            break;
        }

        message.reset();

        // 1. Source a Message from the client
        if (!session->sourceMessage(&message).wait().isOK()) {
            break;
        }

        // 2. Build a sharding request
        Request r(message, session);
        auto txn = cc().makeOperationContext();

        try {
            r.init(txn.get());
            r.process(txn.get());
        } catch (const AssertionException& ex) {
            LOG(ex.isUserAssertion() ? 1 : 0) << "Assertion failed"
                                              << " while processing "
                                              << networkOpToString(message.operation()) << " op"
                                              << " for " << r.getnsIfPresent() << causedBy(ex);
            if (r.expectResponse()) {
                message.header().setId(r.id());
                replyToQuery(ResultFlag_ErrSet, session, message, buildErrReply(ex));
            }

            // We *always* populate the last error for now
            LastError::get(cc()).setLastError(ex.getCode(), ex.what());
        } catch (const DBException& ex) {
            log() << "Exception thrown"
                  << " while processing " << networkOpToString(message.operation()) << " op"
                  << " for " << r.getnsIfPresent() << causedBy(ex);

            if (r.expectResponse()) {
                message.header().setId(r.id());
                replyToQuery(ResultFlag_ErrSet, session, message, buildErrReply(ex));
            }

            // We *always* populate the last error for now
            LastError::get(cc()).setLastError(ex.getCode(), ex.what());
        }

        // Occasionally we want to see if we're using too much memory.
        if ((counter++ & 0xf) == 0) {
            markThreadIdle();
        }
    }
}

}  // namespace mongo
