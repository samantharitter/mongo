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

namespace mongo {

    class StartSessionCommand : public Command {
    public:
        StartSessionCommand() : Command("startSession") {}

        virtual bool supportsWriteConcern(const BSONObj& cmd) const override {
            return false;
        }

        bool slaveOk() const final {
            return false;
        }

        bool adminOnly() const final {
            return true;
        }

        static LogicalSessionRecord makeRecord() {
        }

        Status checkAuthForCommand(Client* client,
                                   const std::string& dbname,
                                   const BSONObj& cmdObj) final {
            // Exactly one user must be authenticated, if auth is enabled.
            auto authzSession = AuthorizationSession::get(client);
            auto it = authzSession->getAuthenticatedUserNames();

            // TODO SERVER-28340 ActionType check
        }

        bool run(OperationContext* opCtx,
                 const std::string& db,
                 const BSONObj& cmdObj,
                 std::string& errmsg,
                 BSONObjBuilder& result) final {
            // Ensure that exactly one user is authenticated.
            // Generate a new session id
            // Make a new session record, with this user as the owner
            // Call start session on the cache
            // Retry if we get a duplicate session error
            // Retry if we get a network error (TODO intelligently)
            // Return the record, with its timeout
            return true;
        }
    } startSessionCmd;

} // namespace mongo
