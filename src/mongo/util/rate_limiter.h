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

#include <cmath>


#include "mongo/stdx/condition_variable.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/duration.h"
#include "mongo/util/time_support.h"

namespace mongo {

/**
 * A logarithmic rate limiter.
 */
class RateLimiter {
public:
    /**
     * f(n) = (scale_y * log_base(scale_x * n - offset_x)) + offset_y
     */
    RateLimiter(double base = std::exp(1),
                double scale_x = 1.0,
                double scale_y = 1.0,
                double offset_x = 0.0,
                double offset_y = 0.0) :
        _base(base),
        _scale_x(scale_x),
        _scale_y(scale_y),
        _offset_x(offset_x),
        _offset_y(offset_y),
        _lastAllowed(Date_t::now()) {}

    static constexpr Milliseconds kCanUseResourceNow{0};

    /**
     * Returns the amount of time the caller should wait before accessing a resource.
     *
     * The given integer should be a meaningful value, such as the number of
     * instances of the given resource that have already been created (for example,
     * the number of connections in a connection pool).
     */
    Milliseconds timeToWait(int n) {
        auto waitUntil = _lastAllowed + Milliseconds{_log(n)};
        auto now = Date_t::now();
        if (waitUntil <= Date_t::now()) {
            return kCanUseResourceNow;
        }

        return waitUntil - now;
    }

    /**
     * Erases the limiter's knowledge of last resource acquisition time, and sets it
     * to the current time.
     */
    void resetTime() {
        stdx::lock_guard<stdx::mutex> lk(_mutex);
        _lastAllowed = Date_t::now();
    }

    /**
     * Sleeps the current thread until allowed to access a resource.
     *
     * The given integer should be a meaningful value, such as the number of
     * instances of the given resource that have already been created (for example,
     * the number of connections in a connection pool).
     */
    void wait(int n) {
        stdx::unique_lock<stdx::mutex> lk(_mutex);
        _cv.wait_for(lk, timeToWait(n).toSystemDuration(), [this, n]{
                return timeToWait(n) == kCanUseResourceNow;
            });

        _lastAllowed = Date_t::now();
    }

private:
    /**
     * Calculates the interval to wait for some input n, in milliseconds.
     */
    int _log(int n) {
        auto res = (int)((_scale_y * (log((_scale_x * n) - _offset_x)/log(_base))) + _offset_y);
        if (res < 0) res = 0;
        return res;
    }

    double _base;
    double _scale_x;
    double _scale_y;
    double _offset_x;
    double _offset_y;

    Date_t _lastAllowed;

    stdx::mutex _mutex;
    stdx::condition_variable _cv;
};

} // namespace mongo
