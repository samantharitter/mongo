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

#include "mongo/db/logical_session_cache.h"

namespace mongo {

LogicalSessionCache::LogicalSessionCache(std::unique_ptr<ServiceLiason> service,
                                         std::unique_ptr<SessionsCollection> collection,
                                         Options options)
    : _refreshInterval(options.refreshInterval),
      _sessionTimeout(options.sessionTimeout),
      _service(std::move(service)),
      _sessionsColl(std::move(collection)),
      _cache(size_t(options.capacity)),
      _running(false) {}

Status LogicalSessionCache::startup() {
    if (_running.swap(true)) {
        return {ErrorCodes::InternalError, "Session cache is already running."};
    }

    _thread = stdx::thread([this]() { _periodicallyRefresh(); });

    return Status::OK();
}

Status LogicalSessionCache::shutdown() {
    if (!_running.swap(false)) {
        return {ErrorCodes::InternalError, "Session cache is not running."};
    }

    _thread.join();

    return Status::OK();
}

void LogicalSessionCache::_refresh_inlock() {
    SessionList activeSessions;
    auto now = _service->now();

    // Assemble a list of active session records in our cache
    {
        stdx::unique_lock<stdx::mutex> lk(_cache_mutex);
        for (auto it : _cache) {
            auto record = it.second;
            // TODO: can we assume these stay in order by lastUse? Hm.
            if (now - record.getlastUse() <= _sessionTimeout) {
                activeSessions.push_back(record.getId());
            }
        }
    }

    // Append any active sessions from the service. If we have cache
    // entries for these, promote them to be recently-used in the cache
    // so we don't lose them to eviction.
    auto serviceSessions = _service->getActiveSessions();
    for (auto it : serviceSessions) {
        _cache.promote(it.first);
    }

    // Query into the sessions collection to do the refresh
    _sessionsColl->refreshSessions(std::move(activeSessions));
}

void LogicalSessionCache::_periodicallyRefresh() {
    while (_running.load()) {
        auto wakeup = _service->now() + _refreshInterval;

        // Wait for the refresh interval
        stdx::unique_lock<stdx::mutex> lk(_cache_mutex);

        // TODO: must use our own internal time here!
        if (_cv.wait_until(lk, wakeup.toSystemTimePoint(), [this]() { return !_running.load(); })) {
            break;
        }

        // Refresh our active sessions against the sessions collection
        _refresh_inlock();
    }
}

}  // namespace mongo
