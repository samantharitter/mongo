/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#include "mongo/platform/basic.h"

#include <boost/optional.hpp>
#include <boost/optional/optional_io.hpp>
#include <third_party/murmurhash3/MurmurHash3.h>

#include "mongo/db/catalog/cursor_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/cursor_id.h"
#include "mongo/db/exec/queued_data_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/db/logical_session_id.h"
#include "mongo/db/namespace_string.h"
#include "mongo/db/operation_context_noop.h"
#include "mongo/db/query/plan_executor.h"
#include "mongo/db/service_context.h"
#include "mongo/db/service_context_noop.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

// Helper, so that we can hold operation context objects in a set.
struct OperationContextHash {
    std::size_t operator()(const OperationContext& opCtx) const {
        uint32_t hash;
        auto id = opCtx.getOpID();
        MurmurHash3_x86_32(&id, sizeof(unsigned int), 0, &hash);
        return hash;
    }
};

    struct OperationContextEqual {
        bool operator()(const OperationContext& lhs, const OperationContext& rhs) const {
            return lhs.getOpID() == rhs.getOpID();
        }
    };

// Helper, so we can hold pinned cursors in a set.
struct ClientCursorPinHash {
    std::size_t operator()(const ClientCursorPin& pin) const {
        uint32_t hash;
        auto id = pin.getCursor()->cursorid();
        MurmurHash3_x86_32(&id, sizeof(CursorId), 0, &hash);
        return hash;
    }
};

    struct ClientCursorPinEqual {
        bool operator()(const ClientCursorPin& lhs, const ClientCursorPin& rhs) const {
            return lhs.getCursor()->cursorid() == rhs.getCursor()->cursorid();
        }
    };

class CursorManagerTest : public mongo::unittest::Test {
public:
    CursorManagerTest()
        : _nss("test.test"),
          _manager(_nss),
          _service(stdx::make_unique<ServiceContextNoop>()),
          _client(_service->makeClient("test")),
          _cmd(BSON("test" << 1)) {}

    CursorManager& manager() {
        return _manager;
    }

    Client* client() const {
        return _client.get();
    }

    ClientCursorPin makeCursor(OperationContext* opCtx) {
        return manager().registerCursor(opCtx, makeParams(opCtx));
    }

    ClientCursorParams makeParams(OperationContext* opCtx) {
        return {makeFakeExecutor(opCtx), _nss, _users, false, _cmd};
    }

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> makeFakeExecutor(OperationContext* opCtx) {
        auto workingSet = stdx::make_unique<WorkingSet>();
        auto queuedDataStage = stdx::make_unique<QueuedDataStage>(opCtx, workingSet.get());
        return unittest::assertGet(PlanExecutor::make(opCtx,
                                                      std::move(workingSet),
                                                      std::move(queuedDataStage),
                                                      _nss,
                                                      PlanExecutor::YieldPolicy::NO_YIELD));
    }

private:
    NamespaceString _nss;

    CursorManager _manager;
    std::unique_ptr<ServiceContextNoop> _service;
    ServiceContext::UniqueClient _client;

    // No-op args for cursor params
    BSONObj _cmd;
    UserNameIterator _users;
};

// Test that cursors inherit the logical session id from their operation context
TEST_F(CursorManagerTest, LogicalSessionIdOnOperationCtxTest) {
    // Cursors created on an op ctx without a session id have no session id
    OperationContextNoop opCtx{client(), 0, boost::none};
    auto pinned = makeCursor(&opCtx);
    ASSERT_EQUALS(pinned.getCursor()->getSessionId(), boost::none);
    manager().invalidateAll(&opCtx, false, "test");

    // Cursors created on an op ctx with a session id have a session id
    auto res = LogicalSessionId::parse("A9A9A9A9-BEDF-4DD9-B001-222345716283");
    ASSERT(res.isOK());
    auto lsid = res.getValue();

    OperationContextNoop opCtx2{client(), 0, lsid};
    auto pinned2 = makeCursor(&opCtx2);
    ASSERT_EQUALS(pinned2.getCursor()->getSessionId(), lsid);
    manager().invalidateAll(&opCtx2, false, "test");
}

TEST_F(CursorManagerTest, CursorsWithoutSessions) {
    // Add a cursor with no session to the cursor manager
    OperationContextNoop opCtx{client(), 0, boost::none};
    auto pinned = makeCursor(&opCtx);
    ASSERT_EQUALS(pinned.getCursor()->getSessionId(), boost::none);

    // Retrieve all sessions active in manager - set should be empty
    auto sessions = manager().getAllSessionsWithActiveCursors();
    ASSERT(sessions.empty());

    manager().invalidateAll(&opCtx, false, "test");
}

TEST_F(CursorManagerTest, OneCursorWithASession) {
    // Add a cursor with a session to the cursor manager
    auto lsid = LogicalSessionId::gen();
    OperationContextNoop opCtx{client(), 0, lsid};
    auto pinned = makeCursor(&opCtx);

    // Retrieve all sessions active in manager - set should contain just lsid
    auto sessions = manager().getAllSessionsWithActiveCursors();
    ASSERT_EQ(sessions.size(), size_t(1));
    auto it = sessions.find(lsid);
    ASSERT_EQ(*it, lsid);

    // Retrieve all cursors for this lsid - should be just ours
    auto cursors = manager().getCursorIdsForSession(lsid);
    ASSERT_EQ(cursors.size(), size_t(1));
    auto cursorId = pinned.getCursor()->cursorid();
    auto it2 = cursors.find(cursorId);
    ASSERT_EQ(*it2, cursorId);

    // Remove the cursor from the manager
    pinned.release();
    ASSERT(manager().eraseCursor(&opCtx, cursorId, false).isOK());

    // There should be no more cursor entries by session id.
    ASSERT(manager().getAllSessionsWithActiveCursors().empty());
    ASSERT(manager().getCursorIdsForSession(lsid).empty());
}

    TEST_F(CursorManagerTest, MultipleCursorsWithSameSession) {
        const int numCursors = 1000;

        auto lsid = LogicalSessionId::gen();

        using CursorState = std::pair<OperationContextNoop, CursorId>;
        std::array<CursorState, numCursors> testCursors;

        //std::unordered_set<OperationContextNoop, OperationContextHash, OperationContextEqual> opCtxs;
        std::unordered_set<ClientCursorPin, ClientCursorPinHash, ClientCursorPinEqual> pins;

        // Add some cursors under the same session to the cursor manager
        for (int i = 0; i < numCursors; i++) {
            OperationContextNoop opCtx(client(), i, lsid);
            auto pin = makeCursor(&opCtx);
            //auto pair = std::make_pair(std::move(opCtx), std::move(pin));
            auto pair = std::make_pair(std::move(opCtx), pin.getCursor()->cursorid());

            testCursors[i] = std::move(pair);
            //auto op = opCtxs.emplace(client(), i, lsid);
            //pins.insert(makeCursor(const_cast<OperationContextNoop*>(&(*(op.first)))));
        }

        // Retrieve all sessions active in manager - set should contain just lsid
        auto sessions = manager().getAllSessionsWithActiveCursors();
        ASSERT_EQ(sessions.size(), size_t(1));
        auto it = sessions.find(lsid);
        ASSERT_EQ(*it, lsid);

        // Retrieve all cursors for this lsid - should be all of ours
        auto cursors = manager().getCursorIdsForSession(lsid);
        ASSERT_EQ(cursors.size(), size_t(numCursors));
        //for (auto& pinned : pins) {
        //    auto it2 = cursors.find(pinned.getCursor()->cursorid());
        //    ASSERT(it2 != cursors.end());
        //}

        // Remove some cursors from the manager
        // go through cursors
        // if removing:
        // 
        // All other cursors should still be retrievable by id

        // Remove all cursors with this lsid, set of lsids should now be empty
        // clear out all remaining cursors
        //for (auto& pinned : pins) {
        //    pinned.release();
        //    manager().eraseCursor(&opCtx, cursorId, false);
        //}
    }

    TEST_F(CursorManagerTest, MultipleCursorsMultipleSessions) {
        // Add some cursors under different sessions to the cursor manager

        // Retrieve all sessions active in manager - should be all of ours

        // For each lsid, we should be able to retrieve all our cursor ids

        // Remove some of the cursors from the manager

        // All other cursors should still be retrievable by id

        // Remove all cursors, set of lsids should now be empty
    }

}  // namespace
}  // namespace mongo
