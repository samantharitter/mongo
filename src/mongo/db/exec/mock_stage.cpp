/**
 *    Copyright (C) 2013 10gen Inc.
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

#include "mongo/db/exec/mock_stage.h"
#include "mongo/db/exec/scoped_timer.h"
#include "mongo/db/exec/working_set_common.h"

namespace mongo {

    const char* MockStage::kStageType = "MOCK";

    MockStage::MockStage(WorkingSet* ws)
        : _ws(ws),
          _commonStats(kStageType)
    {}

    PlanStage::StageState MockStage::work(WorkingSetID* out) {
        ++_commonStats.works;

        // Adds the amount of time taken by work() to executionTimeMillis.
        ScopedTimer timer(&_commonStats.executionTimeMillis);

        if (isEOF()) { return PlanStage::IS_EOF; }

        StageState state = _results.front();
        _results.pop();

        if (PlanStage::ADVANCED == state) {
            ++_commonStats.advanced;
            *out = _members.front();
            _members.pop();
        }
        else if (PlanStage::NEED_TIME == state) {
            ++_commonStats.needTime;
        }

        return state;
    }

    bool MockStage::isEOF() { return _results.empty(); }

    void MockStage::saveState() {
        ++_commonStats.yields;
    }

    void MockStage::restoreState(OperationContext* opCtx) {
        ++_commonStats.unyields;
    }

    void MockStage::invalidate(const DiskLoc& dl, InvalidationType type) {
        ++_commonStats.invalidates;
    }

    PlanStageStats* MockStage::getStats() {
        _commonStats.isEOF = isEOF();
        auto_ptr<PlanStageStats> ret(new PlanStageStats(_commonStats, STAGE_MOCK));
        ret->specific.reset(new MockStats(_specificStats));
        return ret.release();
    }

    const CommonStats* MockStage::getCommonStats() { return &_commonStats; }

    const SpecificStats* MockStage::getSpecificStats() { return &_specificStats; }

    void MockStage::pushBack(const PlanStage::StageState state) {
        invariant(PlanStage::ADVANCED != state);
        _results.push(state);
    }

    void MockStage::pushBack(const WorkingSetMember& member) {
        _results.push(PlanStage::ADVANCED);

        WorkingSetID id = _ws->allocate();
        WorkingSetMember* ourMember = _ws->get(id);
        WorkingSetCommon::initFrom(ourMember, member);

        // member lives in _ws.  We'll return it when _results hits ADVANCED.
        _members.push(id);
    }

    vector<PlanStage*> MockStage::getChildren() const {
        vector<PlanStage*> empty;
        return empty;
    }

}  // namespace mongo
