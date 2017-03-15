/**
 *    Copyright (C) 2017 MongoDB Inc.
 *
 *    This program is free software: you can redistribute it and/or modify
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

#include <regex>

#include "mongo/util/uuid.h"

#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/platform/random.h"
#include "mongo/stdx/mutex.h"
#include "mongo/util/hex.h"

namespace mongo {

namespace {

stdx::mutex uuidGenMutex;
auto uuidGen = SecureRandom::create();

// Regex to match valid version 4 UUIDs with variant bits set
std::regex uuidRegex(
    "[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-4[0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}");

}  // namespace

UUID::UUID(BSONElement from) : _uuid(from.uuid()) {}

UUID::UUID(const std::string& s) {
    invariant(isUUIDString(s));

    // 4 Octets - 2 Octets - 2 Octets - 2 Octets - 6 Octets
    int j = 0;
    for (int i = 0; i < 16; i++) {
        // Skip hyphens
        if (s[j] == '-')
            j++;

        char high = s[j++];
        char low = s[j++];

        _uuid[i] = ((fromHex(high) << 4) | fromHex(low));
    }
}

bool UUID::isUUIDString(const std::string& s) {
    return std::regex_match(s, uuidRegex);
}

UUID UUID::gen() {
    stdx::lock_guard<stdx::mutex> lk(uuidGenMutex);

    // Generate 128 random bits
    int64_t randomWords[2] = {uuidGen->nextInt64(), uuidGen->nextInt64()};
    UUIDStorage randomBytes;
    memcpy(&randomBytes, randomWords, sizeof(randomBytes));

    // Set version in high 4 bits of byte 6 and variant in high 2 bits of byte 8, see RFC 4122,
    // section 4.1.1, 4.1.2 and 4.1.3.
    randomBytes[6] &= 0x0f;
    randomBytes[6] |= 0x40;  // v4
    randomBytes[8] &= 0x3f;
    randomBytes[8] |= 0x80;  // Randomly assigned

    return UUID{randomBytes};
}

BSONObj UUID::toBSON() const {
    BSONObjBuilder builder;
    builder.appendBinData("uuid", sizeof(UUIDStorage), BinDataType::newUUID, &_uuid);
    return builder.obj();
}

std::string UUID::toString() const {
    StringBuilder ss;

    // 4 Octets - 2 Octets - 2 Octets - 2 Octets - 6 Octets
    ss << toHexLower(&_uuid[0], 4);
    ss << "-";
    ss << toHexLower(&_uuid[4], 2);
    ss << "-";
    ss << toHexLower(&_uuid[6], 2);
    ss << "-";
    ss << toHexLower(&_uuid[8], 2);
    ss << "-";
    ss << toHexLower(&_uuid[10], 6);

    return ss.str();
}

}  // namespace mongo
