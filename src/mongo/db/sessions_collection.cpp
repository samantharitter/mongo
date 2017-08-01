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

#include "mongo/db/sessions_collection.h"

#include <memory>

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/memory.h"

namespace mongo {

namespace {

BSONObj lsidQuery(const LogicalSessionId& lsid) {
    return BSON(LogicalSessionRecord::kIdFieldName << lsid.toBSON());
}

BSONObj lsidQuery(const LogicalSessionRecord& record) {
    return lsidQuery(record.getId());
}

BSONObj updateQuery(const LogicalSessionRecord& record, Date_t refreshTime) {
    // { $max : { lastUse : <time> }, $setOnInsert : { user : <user> } }

    // Build our update doc.
    BSONObjBuilder updateBuilder;

    {
        BSONObjBuilder maxBuilder(updateBuilder.subobjStart("$max"));
        maxBuilder.append(LogicalSessionRecord::kLastUseFieldName, refreshTime);
    }

    {
        BSONObjBuilder setBuilder(updateBuilder.subobjStart("$setOnInsert"));
        setBuilder.append(LogicalSessionRecord::kUserFieldName, record.getUser().toBSON());
    }

    return updateBuilder.obj();
}

using InitBatchFn = stdx::function<void(BSONObjBuilder* batch)>;
using FinalizeBatchFn = stdx::function<void(BSONObjBuilder* batch)>;
using SendBatchFn = stdx::function<Status(BSONObj batch)>;

template <typename T>
using AddLineFn = stdx::function<void(BSONArrayBuilder*, const T&)>;

template <template <typename, typename...> class Container, typename T, typename... Types>
Status runBulkCmd(std::string label,
                  InitBatchFn initBatch,
                  AddLineFn<T> addLine,
                  FinalizeBatchFn finalizeBatch,
                  SendBatchFn sendBatch,
                  const Container<T, Types...>& items) {
    int i = 0;
    BufBuilder buf;

    auto batchBuilder = stdx::make_unique<BSONObjBuilder>(buf);
    initBatch(batchBuilder.get());
    auto entries = stdx::make_unique<BSONArrayBuilder>(batchBuilder->subarrayStart(label));

    for (const auto& item : items) {
        addLine(entries.get(), item);

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

SessionsCollection::~SessionsCollection() = default;

SessionsCollection::SendBatchFn SessionsCollection::makeSendFn(DBClientBase* client) {
    auto send = [client](BSONObj batch) -> Status {
        BSONObj res;
        auto ok = client->runCommand(SessionsCollection::kSessionsDb.toString(), batch, res);
        if (!ok) {
            return {ErrorCodes::UnknownError,
                    client->getLastError(SessionsCollection::kSessionsDb.toString())};
        }
        return Status::OK();
    };

    return send;
}

Status SessionsCollection::doRefresh(const LogicalSessionRecordSet& sessions,
                                     Date_t refreshTime,
                                     SendBatchFn send) {
    auto init = [](BSONObjBuilder* batch) { batch->append("update", kSessionsCollection); };

    AddLineFn<LogicalSessionRecord> add = [&refreshTime](BSONArrayBuilder* entries,
                                                         const LogicalSessionRecord& record) {
        entries->append(BSON("q" << lsidQuery(record) << "u" << updateQuery(record, refreshTime)
                                 << "upsert"
                                 << true));
    };

    auto finalize = [](BSONObjBuilder* batch) { batch->append("ordered", false); };

    return runBulkCmd("updates", init, add, finalize, send, sessions);
}

Status SessionsCollection::doRemove(const LogicalSessionIdSet& sessions, SendBatchFn send) {
    auto init = [](BSONObjBuilder* batch) { batch->append("delete", kSessionsCollection); };

    AddLineFn<LogicalSessionId> add = [](BSONArrayBuilder* builder, const LogicalSessionId& lsid) {
        builder->append(BSON("q" << lsidQuery(lsid) << "limit" << 0));
    };

    auto finalize = [](BSONObjBuilder* batch) { batch->append("ordered", false); };

    return runBulkCmd("deletes", init, add, finalize, send, sessions);
}

}  // namespace mongo
