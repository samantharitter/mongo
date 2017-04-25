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

#pragma once

#include "mongo/base/disallow_copying.h"
#include "mongo/base/status_with.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/logical_session_record.h"
#include "mongo/db/service_liason.h"
#include "mongo/db/sessions_collection.h"
#include "mongo/platform/atomic_word.h"
#include "mongo/stdx/thread.h"
#include "mongo/util/lru_cache.h"

namespace mongo {

/**
 * A thread-safe cache structure for logical session records.
 *
 * The cache takes ownership of the passed-in ServiceLiason and
 * SessionsCollection helper types.
 */
class LogicalSessionCache {
    MONGO_DISALLOW_COPYING(LogicalSessionCache);

public:
    using SessionList = std::list<LogicalSessionId>;

    static const int kLogicalSessionCacheDefaultCapacity = 10000;
    static constexpr Minutes kLogicalSessionDefaultTimeout = Minutes(30);
    static constexpr Minutes kLogicalSessionDefaultRefresh = Minutes(5);

    /**
     * An Options type to support the LogicalSessionCache.
     */
    class Options {
    public:
        /**
         * The number of session records to keep in the cache.
         */
        int capacity = kLogicalSessionCacheDefaultCapacity;

        /**
         * A timeout value to use for sessions in the cache, in minutes.
         *
         * By default, this is set to 30 minutes.
         */
        Minutes sessionTimeout = kLogicalSessionDefaultTimeout;

        /**
         * The interval over which the cache will refresh session records.
         *
         * By default, this is set to every 5 minutes. If the caller is
         * setting the sessionTimeout by hand, it is suggested that they
         * consider also setting the refresh interval accordingly.
         */
        Minutes refreshInterval = kLogicalSessionDefaultRefresh;
    };

    /**
     * Construct a new session cache.
     */
    explicit LogicalSessionCache(std::unique_ptr<ServiceLiason> service,
                                 std::unique_ptr<SessionsCollection> collection,
                                 Options options);

    ~LogicalSessionCache();

    /**
     * Start a background thread within the cache that runs periodically to
     * refresh the records in the cache against the sessions collection.
     */
    Status startup();

    /**
     * Shuts down the background thread that refreshes session records.
     */
    Status shutdown();

    /**
     * Return the record for the given session, or return an error if there
     * is no authoritative record for this session.
     *
     * If the cache does not already contain a record for this session, this
     * method may issue networking operations to obtain the record. Afterwards,
     * the cache will keep the record for future use.
     */
    StatusWith<LogicalSessionRecord> getRecord(LogicalSessionId lsid);

    /**
     * Return the record for the given session if we already have it in the
     * cache. Do not fetch the record from the network if we do not already have it.
     */
    StatusWith<LogicalSessionRecord> getRecordFromCache(LogicalSessionId lsid);

    /**
     * Remove all records in this cache. Do not remove the authoritative records
     * from the sessions collection.
     */
    void clear();

private:
    /**
     * Internal methods to handle scheduling and performing refreshes for active
     * session records contained within the cache.
     */
    void _refresh_inlock();
    void _periodicallyRefresh();

    const Minutes _refreshInterval;
    const Minutes _sessionTimeout;

    std::unique_ptr<ServiceLiason> _service;
    std::unique_ptr<SessionsCollection> _sessionsColl;

    stdx::thread _thread;

    stdx::mutex _cache_mutex;
    LRUCache<LogicalSessionId, LogicalSessionRecord, LogicalSessionId::Hash> _cache;

    AtomicBool _running;
};

}  // namespace mongo
