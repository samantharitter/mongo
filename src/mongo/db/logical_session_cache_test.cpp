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

#include "mongo/bson/oid.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/logical_session_cache.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/logical_session_record.h"
#include "mongo/db/service_liason_mock.h"
#include "mongo/db/sessions_collection_mock.h"
#include "mongo/stdx/future.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

const Milliseconds kSessionTimeout =
    duration_cast<Milliseconds>(LogicalSessionCache::kLogicalSessionDefaultTimeout);
const Milliseconds kForceRefresh =
    duration_cast<Milliseconds>(LogicalSessionCache::kLogicalSessionDefaultRefresh);

using SessionList = std::list<LogicalSessionId>;

/**
 * Test fixture that sets up a session cache attached to a mock service liason
 * and mock sessions collection implementation.
 */
class LogicalSessionCacheTest : public unittest::Test {
public:
    LogicalSessionCacheTest()
        : _service(std::make_shared<MockServiceLiasonImpl>()),
          _sessions(std::make_shared<MockSessionsCollectionImpl>()),
          _user("sam", "test"),
          _userId(OID::gen()) {}

    void setUp() override {
        auto mockService = std::make_unique<MockServiceLiason>(_service);
        auto mockSessions = std::make_unique<MockSessionsCollection>(_sessions);
        _cache =
            std::make_unique<LogicalSessionCache>(std::move(mockService), std::move(mockSessions));
    }

    void tearDown() override {
        _service->join();
    }

    LogicalSessionRecord newRecord() {
        return LogicalSessionRecord::makeAuthoritativeRecord(
            LogicalSessionId::gen(), _user, _userId, _service->now());
    }

    std::unique_ptr<LogicalSessionCache>& cache() {
        return _cache;
    }

    std::shared_ptr<MockServiceLiasonImpl> service() {
        return _service;
    }

    std::shared_ptr<MockSessionsCollectionImpl> sessions() {
        return _sessions;
    }

private:
    std::shared_ptr<MockServiceLiasonImpl> _service;
    std::shared_ptr<MockSessionsCollectionImpl> _sessions;

    std::unique_ptr<LogicalSessionCache> _cache;

    UserName _user;
    boost::optional<OID> _userId;
};

// Test that session cache fetches new records from the sessions collection
TEST_F(LogicalSessionCacheTest, CacheFetchesNewRecords) {
    auto record = newRecord();
    auto lsid = record.getLsid();

    // When the record is not present (and not in the sessions collection) returns an error
    auto res = cache()->getOwner(lsid);
    ASSERT(!res.isOK());

    // When the record is not present (but is in the sessions collection) returns it
    sessions()->add(record);
    res = cache()->getOwner(lsid);
    ASSERT(res.isOK());
    ASSERT(res.getValue() == record.getSessionOwner());

    // When the record is present in the cache, returns it
    sessions()->setFetchHook([](LogicalSessionId id) -> StatusWith<LogicalSessionRecord> {
        // We should not be querying the sessions collection on the next call
        ASSERT(false);
        return {ErrorCodes::NoSuchSession, "no such session"};
    });

    res = cache()->getOwner(lsid);
    ASSERT(res.isOK());
    ASSERT(res.getValue() == record.getSessionOwner());
}

// Test that the getFromCache method does not make calls to the sessions collection
TEST_F(LogicalSessionCacheTest, TestCacheHitsOnly) {
    auto record = newRecord();
    auto lsid = record.getLsid();

    // When the record is not present (and not in the sessions collection), returns an error
    auto res = cache()->getOwnerFromCache(lsid);
    ASSERT(!res.isOK());

    // When the record is not present (but is in the sessions collection), returns an error
    sessions()->add(record);
    res = cache()->getOwnerFromCache(lsid);
    ASSERT(!res.isOK());

    // When the record is present, returns the owner
    cache()->getOwner(lsid);
    res = cache()->getOwnerFromCache(lsid);
    ASSERT(res.isOK());
    auto fetched = res.getValue();
    ASSERT(res.getValue() == record.getSessionOwner());
}

// Test that fetching from the cache updates the lastUse date of records
TEST_F(LogicalSessionCacheTest, FetchUpdatesLastUse) {
    auto record = newRecord();
    auto lsid = record.getLsid();

    auto start = service()->now();

    // Insert the record into the sessions collection with 'start'
    record.setLastUse(start);
    sessions()->add(record);

    // Fast forward time and fetch
    service()->fastForward(Milliseconds(500));
    ASSERT(start != service()->now());
    auto res = cache()->getOwner(lsid);
    ASSERT(res.isOK());

    // Now that we fetched, lifetime of session should be extended
    service()->fastForward(kSessionTimeout - Milliseconds(500));
    res = cache()->getOwner(lsid);
    ASSERT(res.isOK());

    // We fetched again, so lifetime extended again
    service()->fastForward(kSessionTimeout - Milliseconds(10));
    res = cache()->getOwner(lsid);
    ASSERT(res.isOK());

    // Fast forward and hit-only fetch
    service()->fastForward(kSessionTimeout - Milliseconds(10));
    res = cache()->getOwnerFromCache(lsid);
    ASSERT(res.isOK());

    // Lifetime extended again
    service()->fastForward(Milliseconds(11));
    res = cache()->getOwnerFromCache(lsid);
    ASSERT(res.isOK());

    // Let record expire, we should not be able to get it from the cache
    service()->fastForward(kSessionTimeout + Milliseconds(1));
    res = cache()->getOwnerFromCache(lsid);
    ASSERT(!res.isOK());
}

// Test the startSession method
TEST_F(LogicalSessionCacheTest, StartSession) {
    auto record = newRecord();
    auto lsid = record.getLsid();

    // Test starting a new session
    auto res = cache()->startSession(record);
    ASSERT(res.isOK());
    ASSERT(sessions()->has(lsid));

    // Try to start a session that is already in the sessions collection and our
    // local cache, should fail
    res = cache()->startSession(record);
    ASSERT(!res.isOK());

    // Try to start a session that is already in the sessions collection but
    // is not in our local cache, should fail
    auto record2 = newRecord();
    sessions()->add(record2);
    res = cache()->startSession(record2);
    ASSERT(!res.isOK());

    // Try to start a session that has expired from our cache, and is no
    // longer in the sessions collection, should succeed
    service()->fastForward(Milliseconds(kSessionTimeout.count() + 5));
    sessions()->remove(lsid);
    ASSERT(!sessions()->has(lsid));
    res = cache()->startSession(record);
    ASSERT(res.isOK());
    ASSERT(sessions()->has(lsid));
}

// Test that records in the cache are properly refreshed until they expire
TEST_F(LogicalSessionCacheTest, CacheRefreshesOwnRecords) {
    // Insert two records into the cache
    auto record1 = newRecord();
    auto record2 = newRecord();
    cache()->startSession(record1);
    cache()->startSession(record2);

    stdx::promise<int> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Advance time to first refresh point, check that refresh happens, and
    // that it includes both our records
    sessions()->setRefreshHook([&hitRefresh](SessionList sessions) -> SessionList {
        hitRefresh.set_value(sessions.size());
        return {};
    });

    // Wait for the refresh to happen
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();
    ASSERT_EQ(refreshFuture.get(), 2);

    sessions()->clearHooks();

    stdx::promise<LogicalSessionId> refresh2;
    auto refresh2Future = refresh2.get_future();

    // Use one of the records
    auto lsid = record1.getLsid();
    auto res = cache()->getOwner(lsid);
    ASSERT(res.isOK());

    // Advance time so that one record expires
    // Ensure that first record was refreshed, and second was thrown away
    sessions()->setRefreshHook([&refresh2](SessionList sessions) -> SessionList {
        // We should only have one record here, the other should have expired
        ASSERT_EQ(sessions.size(), size_t(1));
        refresh2.set_value(sessions.front());
        return {};
    });

    // Wait until the second job has been scheduled
    while (service()->jobs() < 2) {
        sleepmillis(10);
    }

    service()->fastForward(kSessionTimeout - kForceRefresh + Milliseconds(1));
    refresh2Future.wait();
    ASSERT_EQ(refresh2Future.get(), lsid);
}

// Additional tests:
// SERVER-28346
// - Test that cache deletes records that fail to refresh
// - Test that session cache properly expires records after 30 minutes of no use
// - Test that we keep refreshing sessions that are active on the service
// - Test that if we try to refresh a record and it is not in the sessions collection,
//   we remove it from the cache (unless it is active on the service)
// - Test small mixed set of cache/service active sessions
// - Test larger sets of cache-only session records
// - Test larger sets of service-only session records
// - Test larger mixed sets of cache/service active sessions

}  // namespace
}  // namespace mongo
