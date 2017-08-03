/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include "mongo/db/sessions_collection_rs.h"

#include <utility>

#include "mongo/client/connection_string.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/query.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter_factory_impl.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/repl/repl_set_config.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {

BSONObj lsidQuery(const LogicalSessionId& lsid) {
    return BSON(LogicalSessionRecord::kIdFieldName << lsid.toBSON());
}

StatusWith<std::pair<std::string, DBClientBase*>> makePrimaryConnection(OperationContext* opCtx) {
    auto coord = repl::getGlobalReplicationCoordinator();
    auto config = coord->getConfig();
    if (!config.isInitialized()) {
        return {ErrorCodes::NotYetInitialized, "Replication has not yet been configured"};
    }

    // Find the primary
    RemoteCommandTargeterFactoryImpl factory;
    auto targeter = factory.create(config.getConnectionString());
    auto res = targeter->findHost(opCtx, ReadPreferenceSetting(ReadPreference::PrimaryOnly));
    if (!res.isOK()) {
        return res.getStatus();
    }

    auto hostname = res.getValue().toString();

    // Make a connection to the primary, then send
    try {
        ScopedDbConnection conn(hostname);
        return std::make_pair(hostname, conn.release());
    } catch (...) {
        return exceptionToStatus();
    }
}

}  // namespace

StatusWith<LogicalSessionRecord> SessionsCollectionRS::fetchRecord(
    OperationContext* opCtx, const LogicalSessionId& lsid) {

    DBDirectClient client(opCtx);
    auto cursor = client.query(kSessionsFullNS.toString(), lsidQuery(lsid), 1);
    if (!cursor->more()) {
        return {ErrorCodes::NoSuchSession, "No matching record in the sessions collection"};
    }

    try {
        IDLParserErrorContext ctx("LogicalSessionRecord");
        return LogicalSessionRecord::parse(ctx, cursor->next());
    } catch (...) {
        return exceptionToStatus();
    }
}

bool SessionsCollectionRS::isStandaloneOrMaster(OperationContext* opCtx) {
    Lock::DBLock lk(opCtx, SessionsCollection::kSessionsDb, MODE_IX);
    Lock::CollectionLock lock(opCtx->lockState(), SessionsCollection::kSessionsFullNS, MODE_IX);
    auto coord = repl::getGlobalReplicationCoordinator();
    return coord->canAcceptWritesForDatabase(opCtx, SessionsCollection::kSessionsDb);
}

Status SessionsCollectionRS::refreshSessions(OperationContext* opCtx,
                                             const LogicalSessionRecordSet& sessions,
                                             Date_t refreshTime) {
    // If we are the primary, write directly to ourself.
    if (isStandaloneOrMaster(opCtx)) {
        DBDirectClient client(opCtx);
        return doRefresh(sessions, refreshTime, makeSendFn(&client));
    }

    // If we are not writeable, then send refreshSessions cmd to the primary.
    auto res = makePrimaryConnection(opCtx);
    if (!res.isOK()) {
        return res.getStatus();
    }

    ScopedDbConnection conn(res.getValue().first, res.getValue().second);

    return doRefreshExternal(sessions, refreshTime, makeSendFn(conn.get()));
}

Status SessionsCollectionRS::removeRecords(OperationContext* opCtx,
                                           const LogicalSessionIdSet& sessions) {
    // If we are the primary, write directly to ourself.
    if (isStandaloneOrMaster(opCtx)) {
        DBDirectClient client(opCtx);
        return doRemove(sessions, makeSendFn(&client));
    }

    // If we are not writeable, then send endSessions cmd to the primary
    auto res = makePrimaryConnection(opCtx);
    if (!res.isOK()) {
        return res.getStatus();
    }

    ScopedDbConnection conn(res.getValue().first, res.getValue().second);

    return doRemoveExternal(sessions, makeSendFn(conn.get()));
}


}  // namespace mongo
