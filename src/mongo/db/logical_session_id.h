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

#include <string>

#include "mongo/base/status_with.h"
#include "mongo/bson/oid.h"
#include "mongo/db/logical_session_id_gen.h"
#include "mongo/stdx/unordered_set.h"
#include "mongo/util/uuid.h"

namespace mongo {

class BSONObjBuilder;
class OperationContext;

/**
 * An identifier for a logical session. A LogicalSessionId has the following components:
 *
 * - A 128-bit unique identifier (UUID)
 * - An optional user id (ObjectId)
 * - A key id (long long)
 * - An HMAC signature (SHA1Block)
 */
class LogicalSessionId : public Logical_session_id {
public:
    friend class Logical_session_id;
    friend class Logical_session_record;

    using keyIdType = long long;

    /**
     * Create and return a new LogicalSessionId with a random UUID for the given
     * user.
     *
     * The generated LogicalSessionId will not yet be signed, callers should call
     * signLsid() on the LogicalSessionCache separately to generate an HMAC
     * signature for this LogicalSessionId.
     */
    static LogicalSessionId gen(boost::optional<OID> userId = boost::none);

    /**
     * If the given string represents a valid UUID, constructs and returns
     * a new LogicalSessionId for the given user. The returned LogicalSessionId will
     * not yet be signed. Callers should call signLsid() on the LogicalSessionCache
     * separately to generate an HMAC signature for this LogicalSessionId.
     *
     * Otherwise returns an error.
     */
    static StatusWith<LogicalSessionId> parse(const std::string& s,
                                              boost::optional<OID> userId = boost::none);

    /**
     * Constructs a new LogicalSessionId out of a BSONObj. For IDL.
     */
    static LogicalSessionId parse(const BSONObj& doc);

    /**
     * Returns a string representation of this session id.
     */
    std::string toString() const;

    /**
     * Serialize this object to BSON.
     */
    BSONObj toBSON() const;

    inline bool operator==(const LogicalSessionId& rhs) const {
        return getId() == rhs.getId() && getUserId() == rhs.getUserId() &&
            getKeyId() == rhs.getKeyId() && getSignature() == rhs.getSignature();
    }

    inline bool operator!=(const LogicalSessionId& rhs) const {
        return !(*this == rhs);
    }

    /**
     * Custom hasher so LogicalSessionIds can be used in unordered data structures.
     *
     * ex: std::unordered_set<LogicalSessionId, LogicalSessionId::Hash> lsidSet;
     */
    struct Hash {
        std::size_t operator()(const LogicalSessionId& lsid) const {
            return _hasher(lsid.getId());
        }

    private:
        UUID::Hash _hasher;
    };

    /**
     * This constructor exists for IDL only.
     */
    LogicalSessionId();

private:
    /**
     * Construct a LogicalSessionId from a UUID.
     */
    LogicalSessionId(UUID id, boost::optional<OID> userId = boost::none);
};

inline std::ostream& operator<<(std::ostream& s, const LogicalSessionId& lsid) {
    return (s << lsid.toString());
}

inline StringBuilder& operator<<(StringBuilder& s, const LogicalSessionId& lsid) {
    return (s << lsid.toString());
}

/**
 * An alias for sets of session ids.
 */
using LogicalSessionIdSet = stdx::unordered_set<LogicalSessionId, LogicalSessionId::Hash>;

}  // namespace mongo
