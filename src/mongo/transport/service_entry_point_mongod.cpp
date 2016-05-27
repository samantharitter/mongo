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

#include "mongo/transport/service_entry_point_mongod.h"

#include <vector>

#include "mongo/db/client.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/instance.h"
#include "mongo/stdx/thread.h"
#include "mongo/transport/session.h"
#include "mongo/transport/ticket.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/socket_exception.h"
#include "mongo/util/quick_exit.h"

namespace {

using namespace mongo;

// Set up proper headers for formatting an exhaust request, if we need to
bool setExhaustMessage(Message* m, const DbResponse& dbresponse) {
    MsgData::View header = dbresponse.response.header();
    QueryResult::View qr = header.view2ptr();
    long long cursorid = qr.getCursorId();

    if (!cursorid) {
        return false;
    }

    verify(dbresponse.exhaustNS.size() && dbresponse.exhaustNS[0]);

    auto ns = dbresponse.exhaustNS;  // reset() will free this

    m->reset();

    BufBuilder b(512);
    b.appendNum(static_cast<int>(0) /* size set later in appendData() */);
    b.appendNum(header.getId());
    b.appendNum(header.getResponseToMsgId());
    b.appendNum(static_cast<int>(dbGetMore));
    b.appendNum(static_cast<int>(0));
    b.appendStr(ns);
    b.appendNum(static_cast<int>(0));  // ntoreturn
    b.appendNum(cursorid);

    // Pass ownership to m
    m->appendData(b.buf(), b.len());
    b.decouple();

    return true;
}

}  // namespace

namespace mongo {

using transport::Session;
using transport::TransportLayer;

ServiceEntryPointMongod::ServiceEntryPointMongod(TransportLayer* tl) : _tl(tl) {}

void ServiceEntryPointMongod::startSession(Session&& session) {
    stdx::thread t(&ServiceEntryPointMongod::_runSession, this, std::move(session));
    t.detach();
}

void ServiceEntryPointMongod::_runSession(Session&& session) {
    Client::initThread("conn", &session);
    setThreadName(std::string(str::stream() << "conn" << session.id()));

    try {
        // Enter the get-Message, handle-Message, send-Message loop
        _sessionLoop(&session);
    } catch (AssertionException& e) {
        log() << "AssertionException handling request, closing client connection: " << e
              << std::endl;
    } catch (SocketException& e) {
        log() << "SocketException handling request, closing client connection: " << e << std::endl;
    } catch (const DBException& e) {
        // must be right above std::exception to avoid catching subclasses
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

    Client::destroy();
}

void ServiceEntryPointMongod::_sessionLoop(Session* session) {
    Message inMessage;
    bool inExhaust = false;
    int64_t counter = 0;

    while (true) {
        if (inShutdown()) {
            break;
        }

        // 1. Source a Message from the client (unless we are exhausting)
        if (!inExhaust) {
            inMessage.reset();
            if (!session->sourceMessage(&inMessage).wait().isOK()) {
                break;
            }
        }

        // 2. Pass sourced Message up to mongod
        DbResponse dbresponse;
        {
            auto opCtx = getGlobalServiceContext()->makeOperationContext(&cc());
            assembleResponse(opCtx.get(), inMessage, dbresponse, session->remote());

            // opCtx must go out of scope here so that the operation cannot show
            // up in currentOp results after the response reaches the client
        }

        // 3. Format our response, if we have one
        Message& toSink = dbresponse.response;
        if (!toSink.empty()) {
            toSink.header().setId(nextMessageId());
            toSink.header().setResponseToMsgId(inMessage.header().getId());

            // If this is an exhaust cursor, don't source more Messages
            if (dbresponse.exhaustNS.size() > 0 && setExhaustMessage(&inMessage, dbresponse)) {
                log() << "we are in exhaust";
                inExhaust = true;
            } else {
                inExhaust = false;
            }

            // 4. Sink our response to the client
            if (!session->sinkMessage(toSink).wait().isOK()) {
                break;
            }
        } else {
            inExhaust = false;
        }

        // Occasionally we want to see if we're using too much memory.
        if ((counter++ & 0xf) == 0) {
            markThreadIdle();
        }
    }
}

}  // namespace mongo
