/**
 *    Copyright (C) 2017 MongoDB, Inc.
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

#include <memory>

#include "mongo/db/keys_collection_document.h"
#include "mongo/db/keys_collection_manager.h"

namespace mongo {

class OperationContext;
class LogicalTime;
class ServiceContext;

/**
 * This is responsible for providing keys that can be used for HMAC computation. This also supports
 * automatic key rotation that happens on a configurable interval.
 */
class KeysCollectionManagerDirect : public KeysCollectionManager {
public:
    KeysCollectionManagerDirect(std::string purpose, Seconds keyValidForInterval);

    /**
     * Return a key that is valid for the given time and also matches the keyId. Note that this call
     * can block if it will need to do a refresh.
     *
     * Throws ErrorCode::ExceededTimeLimit if it times out.
     */
    StatusWith<KeysCollectionDocument> getKeyForValidation(OperationContext* opCtx,
                                                           long long keyId,
                                                           const LogicalTime& forThisTime) override;

    /**
     * Returns a key that is valid for the given time. Note that unlike getKeyForValidation, this
     * will never do a refresh.
     *
     * Throws ErrorCode::ExceededTimeLimit if it times out.
     */
    StatusWith<KeysCollectionDocument> getKeyForSigning(const LogicalTime& forThisTime) override;

private:
    const std::string _purpose;
    const Seconds _keyValidForInterval;
};

}  // namespace mongo
