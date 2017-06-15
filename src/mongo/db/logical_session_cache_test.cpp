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
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_session_for_test.h"
#include "mongo/db/auth/authz_manager_external_state_mock.h"
#include "mongo/db/auth/authz_session_external_state_mock.h"
#include "mongo/db/auth/user_name.h"
#include "mongo/db/logical_session_cache.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/logical_session_record.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/service_liason_mock.h"
#include "mongo/db/sessions_collection_mock.h"
#include "mongo/stdx/future.h"
#include "mongo/stdx/memory.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

const Milliseconds kSessionTimeout =
    duration_cast<Milliseconds>(LogicalSessionCache::kLogicalSessionDefaultTimeout);
const Milliseconds kForceRefresh =
    duration_cast<Milliseconds>(LogicalSessionCache::kLogicalSessionDefaultRefresh);

using SessionList = std::list<LogicalSessionId>;

class FailureCapableAuthzManagerExternalStateMock : public AuthzManagerExternalStateMock {
public:
    FailureCapableAuthzManagerExternalStateMock() : _findsShouldFail(false) {}
    virtual ~FailureCapableAuthzManagerExternalStateMock() {}

    void setFindsShouldFail(bool enable) {
        _findsShouldFail = enable;
    }

    virtual Status findOne(OperationContext* opCtx,
                           const NamespaceString& collectionName,
                           const BSONObj& query,
                           BSONObj* result) {
        if (_findsShouldFail && collectionName == AuthorizationManager::usersCollectionNamespace) {
            return Status(ErrorCodes::UnknownError,
                          "findOne on admin.system.users set to fail in mock.");
        }
        return AuthzManagerExternalStateMock::findOne(opCtx, collectionName, query, result);
    }

private:
    bool _findsShouldFail;
};

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
        auto mockService = stdx::make_unique<MockServiceLiason>(_service);
        auto mockSessions = stdx::make_unique<MockSessionsCollection>(_sessions);
        _cache =
            stdx::make_unique<LogicalSessionCache>(std::move(mockService), std::move(mockSessions));

        // Set up the auth manager.
        auto localManagerState = stdx::make_unique<FailureCapableAuthzManagerExternalStateMock>();
        _managerState = localManagerState.get();
        _managerState->setAuthzVersion(AuthorizationManager::schemaVersion26Final);
        _authzManager = stdx::make_unique<AuthorizationManager>(std::move(localManagerState));

        // Set up our session state.
        auto localSessionState = stdx::make_unique<AuthzSessionExternalStateMock>(_authzManager.get());
        _sessionState = localSessionState.get();
        _authzSession = stdx::make_unique<AuthorizationSessionForTest>(std::move(localSessionState));
        _authzManager->setAuthEnabled(true);
    }

    void tearDown() override {
        _service->join();
    }

    void waitUntilRefreshScheduled() {
        while (service()->jobs() < 2) {
            sleepmillis(10);
        }
    }

    LogicalSessionRecord newRecord() {
        return newRecord(_user, _userId);
    }

    LogicalSessionRecord newRecord(UserName user, boost::optional<OID> userId = boost::none) {
        return LogicalSessionRecord::makeAuthoritativeRecord(
            LogicalSessionId::gen(), user, userId, _service->now());
    }

    void addUser(OperationContext* opCtx, UserName user) {
        ASSERT_OK(managerState()->insertPrivilegeDocument(opCtx,
                                                          BSON("user" << user.getUser()
                                                               << "db" << user.getDB()
                                                               << "credentials"
                                                               << BSON("MONGODB-CR" << "a")
                                                               << "roles"
                                                               << BSON_ARRAY(BSON("role"
                                                                                  << "readWrite"
                                                                                  << "db" << "test"))),
                                                          BSONObj()));
        ASSERT_OK(authSession()->addAndAuthorizeUser(opCtx, user));
    }

    void addUser(OperationContext* opCtx, UserName user, OID userId) {
        ASSERT_OK(managerState()->insertPrivilegeDocument(opCtx,
                                                          BSON("user" << user.getUser()
                                                               << "db" << user.getDB()
                                                               << "id" << userId
                                                               << "credentials"
                                                               << BSON("MONGODB-CR" << "a")
                                                               << "roles"
                                                               << BSON_ARRAY(BSON("role"
                                                                                  << "readWrite"
                                                                                  << "db" << "test"))),
                                                          BSONObj()));
        ASSERT_OK(authSession()->addAndAuthorizeUser(opCtx, user));
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

    FailureCapableAuthzManagerExternalStateMock* managerState() const {
        return _managerState;
    }

    AuthzSessionExternalStateMock* sessionState() const {
        return _sessionState;
    }

    AuthorizationManager* authManager() const {
        return _authzManager.get();
    }

    AuthorizationSessionForTest* authSession() const {
        return _authzSession.get();
    }

private:
    std::shared_ptr<MockServiceLiasonImpl> _service;
    std::shared_ptr<MockSessionsCollectionImpl> _sessions;

    std::unique_ptr<LogicalSessionCache> _cache;

    UserName _user;
    boost::optional<OID> _userId;

    FailureCapableAuthzManagerExternalStateMock* _managerState;
    AuthzSessionExternalStateMock* _sessionState;
    std::unique_ptr<AuthorizationManager> _authzManager;
    std::unique_ptr<AuthorizationSessionForTest> _authzSession;
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
    sessions()->setRefreshHook([&hitRefresh](LogicalSessionIdSet sessions) {
        hitRefresh.set_value(sessions.size());
        return LogicalSessionIdSet{};
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
    sessions()->setRefreshHook([&refresh2](LogicalSessionIdSet sessions) {
        // We should only have one record here, the other should have expired
        ASSERT_EQ(sessions.size(), size_t(1));
        refresh2.set_value(*(sessions.begin()));
        return LogicalSessionIdSet{};
    });

    // Wait until the second job has been scheduled
    waitUntilRefreshScheduled();

    service()->fastForward(kSessionTimeout - kForceRefresh + Milliseconds(1));
    refresh2Future.wait();
    ASSERT_EQ(refresh2Future.get(), lsid);
}

// Test that cache deletes records that fail to refresh
TEST_F(LogicalSessionCacheTest, CacheDeletesRecordsThatFailToRefresh) {
    // Put two sessions into the cache
    auto record1 = newRecord();
    auto record2 = newRecord();
    cache()->startSession(record1);
    cache()->startSession(record2);

    stdx::promise<void> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Record 1 fails to refresh
    sessions()->setRefreshHook([&hitRefresh, &record1](LogicalSessionIdSet sessions) {
        ASSERT_EQ(sessions.size(), size_t(2));
        hitRefresh.set_value();
        return LogicalSessionIdSet{record1.getLsid()};
    });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();

    // Ensure that one record is still there and the other is gone
    auto res = cache()->getOwnerFromCache(record1.getLsid());
    ASSERT(!res.isOK());
    res = cache()->getOwnerFromCache(record2.getLsid());
    ASSERT(res.isOK());
}

// Test that we don't remove records that fail to refresh if they are active on the service
TEST_F(LogicalSessionCacheTest, KeepActiveSessionAliveEvenIfRefreshFails) {
    // Put two sessions into the cache, one into the service
    auto record1 = newRecord();
    auto record2 = newRecord();
    cache()->startSession(record1);
    service()->add(record1.getLsid());
    cache()->startSession(record2);

    stdx::promise<void> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Record 1 fails to refresh
    sessions()->setRefreshHook([&hitRefresh, &record1](LogicalSessionIdSet sessions) {
        ASSERT_EQ(sessions.size(), size_t(2));
        hitRefresh.set_value();
        return LogicalSessionIdSet{record1.getLsid()};
    });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();

    // Ensure that both records are still there
    auto res = cache()->getOwnerFromCache(record1.getLsid());
    ASSERT(res.isOK());
    res = cache()->getOwnerFromCache(record2.getLsid());
    ASSERT(res.isOK());
}

// Test that session cache properly expires records after 30 minutes of no use
TEST_F(LogicalSessionCacheTest, BasicSessionExpiration) {
    // Insert a record
    auto record = newRecord();
    cache()->startSession(record);
    auto res = cache()->getOwnerFromCache(record.getLsid());
    ASSERT(res.isOK());

    // Force it to expire
    service()->fastForward(Milliseconds(kSessionTimeout.count() + 5));

    // Check that it is no longer in the cache
    res = cache()->getOwnerFromCache(record.getLsid());
    ASSERT(!res.isOK());
}

// Test that we keep refreshing sessions that are active on the service
TEST_F(LogicalSessionCacheTest, LongRunningQueriesAreRefreshed) {
    auto record = newRecord();
    auto lsid = record.getLsid();

    // Insert one active record on the service, none in the cache
    service()->add(lsid);

    stdx::mutex mutex;
    stdx::condition_variable cv;
    int count = 0;

    sessions()->setRefreshHook([&cv, &mutex, &count, &lsid](LogicalSessionIdSet sessions) {
        ASSERT_EQ(*(sessions.begin()), lsid);
        {
            stdx::unique_lock<stdx::mutex> lk(mutex);
            count++;
        }
        cv.notify_all();

        return LogicalSessionIdSet{};
    });

    // Force a refresh, it should refresh our active session
    service()->fastForward(kForceRefresh);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&count] { return count == 1; });
    }

    // Wait until the next job has been scheduled
    waitUntilRefreshScheduled();

    // Force a session timeout, session is still on the service
    service()->fastForward(kSessionTimeout);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&count] { return count == 2; });
    }

    // Wait until the next job has been scheduled
    waitUntilRefreshScheduled();

    // Force another refresh, check that it refreshes that active record again
    service()->fastForward(kForceRefresh);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&count] { return count == 3; });
    }
}

// Test that the set of records we refresh is a sum of cached + active records
TEST_F(LogicalSessionCacheTest, RefreshCachedAndServiceRecordsTogether) {
    // Put one session into the cache, one into the service
    auto record1 = newRecord();
    service()->add(record1.getLsid());
    auto record2 = newRecord();
    cache()->startSession(record2);

    stdx::promise<void> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Both records refresh
    sessions()->setRefreshHook([&hitRefresh](LogicalSessionIdSet sessions) {
        ASSERT_EQ(sessions.size(), size_t(2));
        hitRefresh.set_value();
        return LogicalSessionIdSet{};
    });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();
}

// Test large sets of cache-only session records
TEST_F(LogicalSessionCacheTest, ManyRecordsInCacheRefresh) {
    int count = LogicalSessionCache::kLogicalSessionCacheDefaultCapacity;
    for (int i = 0; i < count; i++) {
        auto record = newRecord();
        cache()->startSession(record);
    }

    stdx::promise<void> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Check that all records refresh
    sessions()->setRefreshHook([&hitRefresh, &count](LogicalSessionIdSet sessions) {
        ASSERT_EQ(sessions.size(), size_t(count));
        hitRefresh.set_value();
        return LogicalSessionIdSet{};
    });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();
}

// Test larger sets of service-only session records
TEST_F(LogicalSessionCacheTest, ManyLongRunningSessionsRefresh) {
    int count = LogicalSessionCache::kLogicalSessionCacheDefaultCapacity;
    for (int i = 0; i < count; i++) {
        auto record = newRecord();
        service()->add(record.getLsid());
    }

    stdx::promise<void> hitRefresh;
    auto refreshFuture = hitRefresh.get_future();

    // Check that all records refresh
    sessions()->setRefreshHook([&hitRefresh, &count](LogicalSessionIdSet sessions) {
        ASSERT_EQ(sessions.size(), size_t(count));
        hitRefresh.set_value();
        return LogicalSessionIdSet{};
    });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    refreshFuture.wait();
}

// Test larger mixed sets of cache/service active sessions
TEST_F(LogicalSessionCacheTest, ManySessionsRefreshComboDeluxe) {
    int count = LogicalSessionCache::kLogicalSessionCacheDefaultCapacity;
    for (int i = 0; i < count; i++) {
        auto record = newRecord();
        service()->add(record.getLsid());

        auto record2 = newRecord();
        cache()->startSession(record2);
    }

    stdx::mutex mutex;
    stdx::condition_variable cv;
    int refreshes = 0;
    int nRefreshed = 0;

    // Check that all records refresh successfully
    sessions()->setRefreshHook(
        [&refreshes, &mutex, &cv, &nRefreshed](LogicalSessionIdSet sessions) {
            {
                stdx::unique_lock<stdx::mutex> lk(mutex);
                refreshes++;
                nRefreshed = sessions.size();
            }
            cv.notify_all();

            return LogicalSessionIdSet{};
        });

    // Force a refresh
    service()->fastForward(kForceRefresh);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&refreshes] { return refreshes == 1; });
    }
    ASSERT_EQ(nRefreshed, count * 2);

    // Remove all of the service sessions, should just refresh the cache entries
    // (and make all but one fail to refresh)
    service()->clear();
    sessions()->setRefreshHook(
        [&refreshes, &mutex, &cv, &nRefreshed](LogicalSessionIdSet sessions) {
            {
                stdx::unique_lock<stdx::mutex> lk(mutex);
                refreshes++;
                nRefreshed = sessions.size();
            }
            cv.notify_all();

            sessions.erase(sessions.begin());
            return sessions;
        });

    // Wait for job to be scheduled
    waitUntilRefreshScheduled();

    // Force another refresh
    service()->fastForward(kForceRefresh);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&refreshes] { return refreshes == 2; });
    }

    // We should not have refreshed any sessions from the service, only the cache
    ASSERT_EQ(nRefreshed, count);

    // Wait for job to be scheduled
    waitUntilRefreshScheduled();

    // Force a third refresh
    service()->fastForward(kForceRefresh);
    {
        stdx::unique_lock<stdx::mutex> lk(mutex);
        cv.wait(lk, [&refreshes] { return refreshes == 3; });
    }

    // Since all but one record failed to refresh, third set should just have one record
    ASSERT_EQ(nRefreshed, 1);
}

    // TEST_F(LogicalSessionCacheTest, AuthCheckPassesWhenAuthIsOff) {
    //     auto record = newRecord();
    //     auto lsid = record.getLsid();
    //     ASSERT_OK(cache()->startSession(record));
    //     authManager()->setAuthEnabled(false);
    //     ASSERT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    // }

    // TODO un-owned records, for when auth is off?

    TEST_F(LogicalSessionCacheTest, AuthCheckFailsWithNoUsers) {
        auto record = newRecord();
        auto lsid = record.getLsid();
        ASSERT_OK(cache()->startSession(record));
        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

    TEST_F(LogicalSessionCacheTest, AuthCheckFailsWithTooManyUsers) {
        OperationContextNoop opCtx;

        // Add two users
        UserName sam("sam", "test");
        UserName jeannette("jeannette", "test");
        addUser(&opCtx, sam);
        addUser(&opCtx, jeannette);

        // Insert a session owned by one user.
        auto record = newRecord(sam);
        auto lsid = record.getLsid();
        ASSERT_OK(cache()->startSession(std::move(record)));

        // Auth check should fail, too many users.
        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

    TEST_F(LogicalSessionCacheTest, AuthCheckFailsIfSessionDoesntExist) {
        OperationContextNoop opCtx;

        UserName sam("sam", "test");
        addUser(&opCtx, sam);

        auto lsid = LogicalSessionId::gen();

        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

    TEST_F(LogicalSessionCacheTest, AuthCheckFailsIfUserNamesDontMatch) {
        // Add a record owned by "sam" to the cache.
        UserName sam("sam", "test");
        auto record = newRecord(sam);
        auto lsid = record.getLsid();
        ASSERT_OK(cache()->startSession(std::move(record)));

        // Attempt to use the record when not-sam is authenticated.
        UserName notSam("notSam", "test");
        OperationContextNoop opCtx;
        addUser(&opCtx, notSam);
        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

    TEST_F(LogicalSessionCacheTest, AuthCheckFailsIfUserIdsDontMatch) {
        OperationContextNoop opCtx;

        // Add a record owned by "sam" with some id to the cache.
        UserName sam("sam", "test");

        auto record = newRecord(sam, OID::gen());
        auto lsid = record.getLsid();
        ASSERT_OK(cache()->startSession(std::move(record)));

        // Attempt to use the record when "sam" is authenticated with no id.
        addUser(&opCtx, sam);
        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));

        authSession()->logoutDatabase("test");

        // Attempt to use the record when "sam" is authenticated with a different id.
        addUser(&opCtx, sam, OID::gen());
        ASSERT_NOT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

    TEST_F(LogicalSessionCacheTest, SuccessfulAuthCheckUpdatesLastUse) {
        OperationContextNoop opCtx;

        // Add a record owned by "sam" with some id to the cache.
        auto oid = OID::gen();
        UserName sam("sam", "test");

        auto record = newRecord(sam, oid);
        auto lsid = record.getLsid();
        ASSERT_OK(cache()->startSession(std::move(record)));

        // Fast forward time
        //service()->fastForward(Milliseconds(500));

        // Authenticate as "sam" and attempt to use
        addUser(&opCtx, sam, oid);
        ASSERT_OK(cache()->performSessionAuthCheck(authSession(), lsid));

        // Now that we did an auth check, lifetime of session should extend
        //service()->fastForward(kSessionTimeout - Milliseconds(500));
        //ASSERT_OK(cache()->performSessionAuthCheck(authSession(), lsid));

        // Fast forward again and fetch, should still be there
        //service()->fastForward(Milliseconds(1000));
        //ASSERT_OK(cache()->performSessionAuthCheck(authSession(), lsid));
    }

}  // namespace
}  // namespace mongo
