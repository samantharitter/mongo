/**
 *    Copyright (C) 2017 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include <ostream>

#include "mongo/db/sessions_collection_standalone.h"

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/query.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"

namespace mongo {

namespace {

const char kSessionsCollection[] = "system.sessions";
const char kSessionsFullNS[] = "admin.system.sessions";
const char kLsidField[] = "_id.lsid";

BSONObj lsidQuery(LogicalSessionId lsid) {
    return BSON(kLsidField << lsid.toBSON());
}

}  // namespace

StatusWith<LogicalSessionRecord> SessionsCollectionStandalone::fetchRecord(
    OperationContext* opCtx, SignedLogicalSessionId slsid) {
    DBDirectClient client(opCtx);
    auto cursor = client.query(kSessionsFullNS, lsidQuery(slsid.getLsid()), 1);
    if (!cursor->more()) {
        return {ErrorCodes::NoSuchSession, "No matching record in the sessions collection"};
    }

    return LogicalSessionRecord::parse(cursor->next());
}

Status SessionsCollectionStandalone::insertRecord(OperationContext* opCtx,
                                                  LogicalSessionRecord record) {
    DBDirectClient client(opCtx);

    client.insert(kSessionsFullNS, record.toBSON());
    auto errorString = client.getLastError();
    if (errorString.empty()) {
        return Status::OK();
    }

    // TODO: what if there is a different error?
    return {ErrorCodes::DuplicateSession, errorString};
}

Status SessionsCollectionStandalone::refreshSessions(OperationContext* opCtx,
                                                     LogicalSessionIdSet sessions,
                                                     Date_t refreshTime) {
    // Build our update doc.
    BSONObjBuilder updateBuilder;
    {
        BSONObjBuilder maxBuilder(updateBuilder.subobjStart("$max"));
        maxBuilder.append("lastUse", refreshTime);
    }
    auto update = updateBuilder.done();

    // Build our bulk doc.
    BSONObjBuilder bulkBuilder;
    bulkBuilder.append("update", kSessionsCollection);
    {
        BSONArrayBuilder updates(bulkBuilder.subarrayStart("updates"));
        for (auto& lsid : sessions) {
            updates.append(BSON("q" << lsidQuery(lsid) << "u" << update));
        }
    }
    bulkBuilder.append("ordered", false);

    DBDirectClient client(opCtx);
    BSONObj res;
    auto ok = client.runCommand("admin", bulkBuilder.done(), res);
    if (!ok) {
        return {ErrorCodes::UnknownError, client.getLastError("admin")};
    }

    return Status::OK();
}

Status SessionsCollectionStandalone::removeRecords(OperationContext* opCtx,
                                                   LogicalSessionIdSet sessions) {

    // Build our bulk doc.
    BSONObjBuilder bulkBuilder;
    bulkBuilder.append("delete", kSessionsCollection);
    {
        BSONArrayBuilder updates(bulkBuilder.subarrayStart("deletes"));
        for (auto& lsid : sessions) {
            updates.append(BSON("q" << lsidQuery(lsid) << "limit" << 0));
        }
    }
    bulkBuilder.append("ordered", false);

    DBDirectClient client(opCtx);
    BSONObj res;
    auto ok = client.runCommand("admin", bulkBuilder.done(), res);
    if (!ok) {
        return {ErrorCodes::UnknownError, client.getLastError("admin")};
    }

    if (res["writeErrors"]) {
        return {ErrorCodes::UnknownError, "unable to remove all session records"};
    }

    return Status::OK();
}

}  // namespace mongo
