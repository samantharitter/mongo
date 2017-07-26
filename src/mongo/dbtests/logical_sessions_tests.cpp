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

#include <memory>

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/index_spec.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/logical_session_record.h"
#include "mongo/db/operation_context.h"
#include "mongo/db/sessions_collection_standalone.h"
#include "mongo/db/signed_logical_session_id.h"
#include "mongo/dbtests/dbtests.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/time_support.h"

namespace LogicalSessionTests {

namespace {
constexpr StringData kTestNS = "admin.system.sessions"_sd;

LogicalSessionRecord makeRecord() {
    return LogicalSessionRecord::makeAuthoritativeRecord(SignedLogicalSessionId::gen(),
                                                         Date_t::now());
}

    Status insertRecord(OperationContext* opCtx, LogicalSessionRecord record) {
        DBDirectClient client(opCtx);

        client.insert(kTestNS.toString(), record.toBSON());
        auto errorString = client.getLastError();
        if (errorString.empty()) {
            return Status::OK();
        }

        return {ErrorCodes::DuplicateSession, errorString};
    }

}  // namespace

class SessionsCollectionStandaloneTest {
public:
    SessionsCollectionStandaloneTest()
        : _collection(stdx::make_unique<SessionsCollectionStandalone>()) {
        _opCtx = cc().makeOperationContext();
        DBDirectClient db(opCtx());
        db.dropCollection(kTestNS.toString());
    }

    virtual ~SessionsCollectionStandaloneTest() {
        DBDirectClient db(opCtx());
        db.dropCollection(kTestNS.toString());
        _opCtx.reset();
    }

    SessionsCollectionStandalone* collection() {
        return _collection.get();
    }

    OperationContext* opCtx() {
        return _opCtx.get();
    }

private:
    std::unique_ptr<SessionsCollectionStandalone> _collection;
    ServiceContext::UniqueOperationContext _opCtx;
};

// Test that removal from this collection works.
class SessionsCollectionStandaloneRemoveTest : public SessionsCollectionStandaloneTest {
public:
    void run() {
        auto record1 = makeRecord();
        auto record2 = makeRecord();

        auto res = insertRecord(opCtx(), record1);
        ASSERT_OK(res);
        res = insertRecord(opCtx(), record2);
        ASSERT_OK(res);

        // Remove one record, the other stays
        res = collection()->removeRecords(opCtx(), {record1.getId().getLsid()});
        ASSERT_OK(res);

        auto swRecord = collection()->fetchRecord(opCtx(), record1.getId());
        ASSERT(!swRecord.isOK());

        swRecord = collection()->fetchRecord(opCtx(), record2.getId());
        ASSERT(swRecord.isOK());
    }
};

// Test that refreshing entries in this collection works.
class SessionsCollectionStandaloneRefreshTest : public SessionsCollectionStandaloneTest {
public:
    void run() {
        // Attempt to refresh one active record, should succeed.
        auto now = Date_t::now();
        auto record1 = LogicalSessionRecord::makeAuthoritativeRecord(SignedLogicalSessionId::gen(),
                                                                     now - Minutes(5));
        auto res = insertRecord(opCtx(), record1);
        ASSERT_OK(res);
        auto resRefresh = collection()->refreshSessions(opCtx(), {record1.getId().getLsid()}, now);
        ASSERT(resRefresh.isOK());

        // The timestamp on the refreshed record should be updated.
        auto swRecord = collection()->fetchRecord(opCtx(), record1.getId());
        ASSERT(swRecord.isOK());
        ASSERT_EQ(swRecord.getValue().getLastUse(), now);

        // Attempt to refresh a record that is not present, should fail.
        auto lsid2 = LogicalSessionId::gen();
        resRefresh = collection()->refreshSessions(opCtx(), {lsid2}, Date_t::now());
        ASSERT(resRefresh.isOK());

        LogicalSessionIdSet toRefresh;
        for (int i = 0; i < 1000; i++) {
            auto record = makeRecord();
            res = insertRecord(opCtx(), record);

            // Refresh some of these records.
            if (i % 4 == 0) {
                toRefresh.insert(record.getId().getLsid());
            }
        }

        // Add some records to the refresh set that do not exist in the collection.
        int shouldFail = 100;
        for (int i = 0; i < shouldFail; i++) {
            toRefresh.insert(LogicalSessionId::gen());
        }

        // Run the refresh: some should pass, and some should fail.
        resRefresh = collection()->refreshSessions(opCtx(), toRefresh, Date_t::now());
        ASSERT(resRefresh.isOK());
    }
};

class All : public Suite {
public:
    All() : Suite("logical_sessions") {}

    void setupTests() {
        add<SessionsCollectionStandaloneRemoveTest>();
        add<SessionsCollectionStandaloneRefreshTest>();
    }
};

SuiteInstance<All> all;

}  // namespace LogicalSessionTests
