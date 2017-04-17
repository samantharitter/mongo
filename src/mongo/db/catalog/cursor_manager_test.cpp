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

#include "mongo/db/catalog/cursor_manager.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
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

class CursorManagerTest : public mongo::unittest::Test {
public:
    CursorManagerTest()
        : _manager(NamespaceString{"test.test"}),
          _service(stdx::make_unique<ServiceContextNoop>()),
          _client(_service->makeClient("test")) {}

    CursorManager& manager() {
        return _manager;
    }

    Client* client() const {
        return _client.get();
    }

    std::unique_ptr<PlanExecutor, PlanExecutor::Deleter> makeFakeExecutor(OperationContext* opCtx) {
        auto workingSet = stdx::make_unique<WorkingSet>();
        auto queuedDataStage = stdx::make_unique<QueuedDataStage>(opCtx, workingSet.get());
        return unittest::assertGet(PlanExecutor::make(opCtx,
                                                      std::move(workingSet),
                                                      std::move(queuedDataStage),
                                                      NamespaceString{"test.test"},
                                                      PlanExecutor::YieldPolicy::NO_YIELD));
    }

private:
    CursorManager _manager;
    std::unique_ptr<ServiceContextNoop> _service;
    ServiceContext::UniqueClient _client;
};

// Test that cursors inherit the logical session id from their operation context
TEST_F(CursorManagerTest, LogicalSessionIdOnOperationCtxTest) {
    auto cmd = BSON("test" << 1);
    NamespaceString nss{"test.test"};
    UserNameIterator users{};

    // Cursors created on an op ctx without a session id have no session id
    OperationContextNoop opCtx{client(), 0, boost::none};
    ClientCursorParams params{makeFakeExecutor(&opCtx), nss, users, false, cmd};
    auto pinned = manager().registerCursor(&opCtx, std::move(params));
    ASSERT_EQUALS(pinned.getCursor()->getSessionId(), boost::none);
    manager().invalidateAll(&opCtx, false, "test");

    // Cursors created on an op ctx with a session id have a session id
    auto res = LogicalSessionId::parse("A9A9A9A9-BEDF-4DD9-B001-222345716283");
    ASSERT(res.isOK());
    auto lsid = res.getValue();

    OperationContextNoop opCtx2{client(), 0, lsid};
    ClientCursorParams params2{makeFakeExecutor(&opCtx2), nss, users, false, cmd};
    auto pinned2 = manager().registerCursor(&opCtx2, std::move(params2));
    ASSERT_EQUALS(pinned2.getCursor()->getSessionId(), lsid);
    manager().invalidateAll(&opCtx2, false, "test");
}

}  // namespace
}  // namespace mongo
