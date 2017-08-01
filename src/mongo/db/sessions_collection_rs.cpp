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

#include "mongo/client/connection_string.h"
#include "mongo/client/dbclientinterface.h"
#include "mongo/client/query.h"
#include "mongo/client/read_preference.h"
#include "mongo/client/remote_command_targeter_factory_impl.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {

BSONObj lsidQuery(const LogicalSessionId& lsid) {
    return BSON(LogicalSessionRecord::kIdFieldName << lsid.toBSON());
}
}  // namespace

SessionsCollectionRS::SessionsCollectionRS(ConnectionString cs) {
    setConnectionString(cs);
}

    void SessionsCollectionRS::setConnectionString(ConnectionString cs) {
        {
            stdx::lock_guard<stdx::mutex> lock(_mutex);
            if (_cs == cs) { return; }

            RemoteCommandTargeterFactoryImpl factory;
            _cs = cs;
            _targeter.reset(factory.create(cs).release());
        }
    }

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

bool SessionsCollectionRS::isStandaloneOrMaster() {
    if (!_cs.isValid()) {
        return true;
    }
}

StatusWith<HostAndPort> SessionsCollectionRS::findMaster_inlock(OperationContext* opCtx) {
    // If we don't have a valid connection string, then we aren't acting as a
    // replica set member; direct the write to ourselves.
    if (!_cs.isValid()) {
        return my address;
    }

    return _targeter->findHost(opCtx, ReadPreferenceSetting(ReadPreference::PrimaryOnly));
}

StatusWith<std::unique_ptr<DBClientBase>> SessionsCollectionRS::makeConnection(OperationContext* opCtx) {
    {
        stdx::lock_guard<stdx::mutex> lock(_mutex);

        // If we are currently acting as a standalone, return a dbdirect client connection.
        if (!_cs.isValid()) {
            return stdx::make_unique<DBDirectClient>(opCtx);
            DBDirectClient client(opCtx);
            return stdx::make_unique<ScopedDbConnection>("self", 
        }

    auto res = findMaster(opCtx);
    if (!res.isOK()) {
        return res.getStatus();
    }

    try {
        auto conn = stdx::make_unique<ScopedDbConnection>(res.getValue().toString());
        return conn;
    } catch (...) {
        return exceptionToStatus();
    }
}

Status SessionsCollectionRS::refreshSessions(OperationContext* opCtx,
                                             const LogicalSessionRecordSet& sessions,
                                             Date_t refreshTime) {
    if (isCurrentlyStandalone()) {
        DBDirectClient client(opCtx);
        return doRefresh(sessions, refreshTime, makeSendFn(&client));
    }

    auto res = makeConnection(opCtx);
    if (!res.isOK()) {
        return res.getStatus();
    }

    return doRefresh(sessions, refreshTime, makeSendFn(&(res.getValue()->conn())));
}

Status SessionsCollectionRS::removeRecords(OperationContext* opCtx,
                                           const LogicalSessionIdSet& sessions) {
    if (isCurrentlyStandalone()) {
        DBDirectClient client(opCtx);
        return doRefresh(sessions, refreshTime, makeSendFn(&client));
    }

    auto res = makeConnection(opCtx);
    if (!res.isOK()) {
        return res.getStatus();
    }

    return doRemove(sessions, makeSendFn(&(res.getValue()->conn())));
}


}  // namespace mongo
