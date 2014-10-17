/**
 *    Copyright (C) 2014 MongoDB Inc.
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

//
// This file contains tests for mongo/db/exec/mock_stage.cpp
//

#include "mongo/db/exec/mock_stage.h"
#include "mongo/db/exec/working_set.h"
#include "mongo/unittest/unittest.h"

using namespace mongo;

namespace {

    //
    // Basic test that we get out valid stats objects.
    //
    TEST(MockStageTest, getValidStats) {
        WorkingSet ws;
        auto_ptr<MockStage> mock(new MockStage(&ws));
        const CommonStats* commonStats = mock->getCommonStats();
        ASSERT_EQUALS(commonStats->works, static_cast<size_t>(0));
        const SpecificStats* specificStats = mock->getSpecificStats();
        ASSERT(specificStats);
        auto_ptr<PlanStageStats> allStats(mock->getStats());
        ASSERT_EQUALS(allStats->stageType, mock->stageType());
    }

    //
    // Test that our stats are updated as we perform operations.
    //
    TEST(MockStageTest, validateStats) {
        WorkingSet ws;
        WorkingSetID wsID;
        auto_ptr<MockStage> mock(new MockStage(&ws));

        // to avoid lots of static_cast<size_t>
        size_t zero = 0;
        size_t one = 1;
        size_t two = 2;

        // make sure that we're at all zeros
        const CommonStats* stats = mock->getCommonStats();
        ASSERT_EQUALS(stats->yields, zero);
        ASSERT_EQUALS(stats->unyields, zero);
        ASSERT_EQUALS(stats->invalidates, zero);
        ASSERT_EQUALS(stats->works, zero);
        ASSERT_EQUALS(stats->needTime, zero);
        ASSERT_EQUALS(stats->advanced, zero);
        ASSERT_FALSE(stats->isEOF);

        // 'perform' some operations, validate stats
        // needTime
        mock->pushBack(PlanStage::NEED_TIME);
        mock->work(&wsID);
        ASSERT_EQUALS(stats->works, one);
        ASSERT_EQUALS(stats->needTime, one);

        // advanced, with pushed data
        const WorkingSetMember member;
        mock->pushBack(member);
        mock->work(&wsID);
        ASSERT_EQUALS(stats->works, two);
        ASSERT_EQUALS(stats->advanced, one);

        // yields
        mock->saveState();
        ASSERT_EQUALS(stats->yields, one);

        // unyields
        mock->restoreState(NULL);
        ASSERT_EQUALS(stats->unyields, one);

        // invalidates
        const DiskLoc dl(0, 0);
        mock->invalidate(dl, INVALIDATION_MUTATION);
        ASSERT_EQUALS(stats->invalidates, one);

        // and now we are done, but must trigger EOF with getStats()
        ASSERT_FALSE(stats->isEOF);
        auto_ptr<PlanStageStats> allStats(mock->getStats());
        ASSERT_TRUE(stats->isEOF);
    }
}
