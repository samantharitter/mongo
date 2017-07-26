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

#include "mongo/db/sessions_collection_standalone.h"

#include <memory>

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/query.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbhelpers.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {

constexpr StringData kSessionsCollection = "system.sessions"_sd;
constexpr StringData kSessionsFullNS = "admin.system.sessions"_sd;
constexpr StringData kLsidField = "_id.lsid"_sd;

BSONObj lsidQuery(const LogicalSessionId& lsid) {
    return BSON(kLsidField << lsid.toBSON());
}

using InitBatchFn = stdx::function<void(BSONObjBuilder* batch)>;
using AddLineFn = stdx::function<void(BSONArrayBuilder*, const LogicalSessionId&)>;
using FinalizeBatchFn = stdx::function<void(BSONObjBuilder* batch)>;
using SendBatchFn = stdx::function<Status(BSONObj batch)>;

Status runBulkCmd(std::string label,
                  InitBatchFn initBatch,
                  AddLineFn addLine,
                  FinalizeBatchFn finalizeBatch,
                  SendBatchFn sendBatch,
                  const LogicalSessionIdSet& sessions) {
    int i = 0;
    BufBuilder buf;

    // use optional?
    auto batchBuilder = stdx::make_unique<BSONObjBuilder>(buf);
    initBatch(batchBuilder.get());
    auto entries = stdx::make_unique<BSONArrayBuilder>(batchBuilder->subarrayStart(label));

    for (const auto& lsid : sessions) {
        addLine(entries.get(), lsid);
        if (++i >= 1000) {
            entries->done();
            finalizeBatch(batchBuilder.get());

            auto res = sendBatch(batchBuilder->done());
            if (!res.isOK()) {
                return res;
            }

            buf.reset();
            batchBuilder.reset(new BSONObjBuilder(buf));
            initBatch(batchBuilder.get());
            entries.reset(new BSONArrayBuilder(batchBuilder->subarrayStart(label)));
            i = 0;
        }
    }

    entries->done();
    finalizeBatch(batchBuilder.get());
    return sendBatch(batchBuilder->done());
}

}  // namespace

StatusWith<LogicalSessionRecord> SessionsCollectionStandalone::fetchRecord(
    OperationContext* opCtx, SignedLogicalSessionId slsid) {
    DBDirectClient client(opCtx);
    auto cursor = client.query(kSessionsFullNS.toString(), lsidQuery(slsid.getLsid()), 1);
    if (!cursor->more()) {
        return {ErrorCodes::NoSuchSession, "No matching record in the sessions collection"};
    }

    return LogicalSessionRecord::parse(cursor->next());
}

Status SessionsCollectionStandalone::refreshSessions(OperationContext* opCtx,
                                                     const LogicalSessionIdSet& sessions,
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
        for (const auto& lsid : sessions) {
            updates.append(BSON("q" << lsidQuery(lsid) << "u" << update << "upsert" << true));
        }
    }
    bulkBuilder.append("ordered", false);

    auto initializeBuilder = [](BSONObjBuilder* batch) {
        batch->append("update", kSessionsCollection);
    };

    auto addLine = [&update](BSONArrayBuilder* entries, const LogicalSessionId& lsid) {
        entries->append(BSON("q" << lsidQuery(lsid) << "u" << update << "upsert" << true));
    };

    auto finalize = [](BSONObjBuilder* batch) {
        batch->append("ordered", false);
    };

    DBDirectClient client(opCtx);
    auto sendBatch = [&client](BSONObj batch) -> Status {
        BSONObj res;
        auto ok = client.runCommand("admin", batch, res);
        if (!ok) {
            return {ErrorCodes::UnknownError, client.getLastError("admin")};
        }

        return Status::OK();
    };

    return sendBatch(bulkBuilder.done());
}

Status SessionsCollectionStandalone::removeRecords(OperationContext* opCtx,
                                                   const LogicalSessionIdSet& sessions) {
    auto init = [](BSONObjBuilder* batch) {
        batch->append("delete", kSessionsCollection);
    };

    auto add = [](BSONArrayBuilder* builder, const LogicalSessionId& lsid) {
        builder->append(BSON("q" << lsidQuery(lsid) << "limit" << 0));
    };

    auto finalize = [](BSONObjBuilder* batch) {
        batch->append("ordered", false);
    };

    DBDirectClient client(opCtx);
    auto send = [&client](BSONObj batch) -> Status {
        BSONObj res;
        auto ok = client.runCommand("admin", std::move(batch), res);
        if (!ok) {
            return {ErrorCodes::UnknownError, client.getLastError("admin")};
        }

        if (res["writeErrors"]) {
            return {ErrorCodes::UnknownError, "unable to remove all session records"};
        }

        return Status::OK();
    };

    return runBulkCmd("deletes", init, add, finalize, send, sessions);
}

}  // namespace mongo
