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
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

//
// This file tests db/exec/index_scan.cpp
//

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/index_scan.h"
#include "mongo/db/exec/basic_concurrency_test.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/dbtests/dbtests.h"

namespace QueryStageIXScan {

    // messy but necessary
    vector<DiskLoc> g_locs;
    Collection* g_coll;
    OperationContext* g_txn;
    int g_numObj;

    namespace {

        //
        // Call work() on this IndexScan until we have produced a document
        // or reached EOF or otherwise failed.
        //
        void produce(PlanStage* tree) {
            WorkingSetID wsOut;
            PlanStage::StageState state = tree->work(&wsOut);
            while (PlanStage::ADVANCED != state &&
                   PlanStage::IS_EOF != state) {
                // call work
                state = tree->work(&wsOut);
                if (PlanStage::FAILURE == state) return;
            }
            return;
        }

        //
        // Get an upcoming DiskLoc
        //
        DiskLoc getDiskLoc(PlanStage* tree, int nProduced, bool match) {
            // we need to cycle here in case documents we added
            // during setup have been deleted or moved.
            int tries = 0;
            while (tries + nProduced < g_numObj) {
                DiskLoc dl;
                if (match) dl = g_locs[nProduced];
                else {
                    // assume our bounds are 50 -> 100 for all tests
                    if (tries > 50) break;
                    dl = g_locs[tries];
                }
                // check that we can get a doc for this diskloc
                BSONObj target = g_coll->docFor(g_txn, dl);
                if (!target.isEmpty()) return dl;
            }
            // we shouldn't get here, if we did we need to seed more docs
            invariant(false);
        }

        //
        // Return a document to either match or not match, depending on 'match'.
        // If it matches it should be in a part of the index we haven't yet
        // seen.
        //
        BSONObj getNewDocument(int nProduced, bool match) {
            // if we hit this, need to seed more things into collection
            invariant(nProduced < g_numObj);

            if (match) return BSON("a" << nProduced + 1);
            else return BSON("a" << -nProduced); // nProduced will always be > 0
        }
    }

    class QueryStageIXScanBase {
    public:
        QueryStageIXScanBase() : _client(&_txn) {}

        virtual ~QueryStageIXScanBase() {
            Client::WriteContext ctx(&_txn, ns());
            _client.dropCollection(ns());
            ctx.commit();
        }

        void setup() {
            Client::WriteContext ctx(&_txn, ns());

            // set up our globals
            g_numObj = 100;
            g_coll = ctx.ctx().db()->getCollection(&_txn, ns());
            g_txn = &_txn;

            for (int i = 0; i < g_numObj; i++) {
                insert(BSON("a" << i));
            }
            addIndex(BSON("a" << 1));

            ctx.commit();
        }

        void insert(const BSONObj& obj) {
            _client.insert(ns(), obj);
        }

        void addIndex(const BSONObj& obj) {
            _client.ensureIndex(ns(), obj);
        }

        //
        // Given a PlanStage, exhaust it and collect its disklocs.
        //
        void getLocs(PlanStage* scan, WorkingSet* ws, vector<DiskLoc>* out) {
            // NOTE: check for failure and dead here, too?
            while (!scan->isEOF()) {
                WorkingSetID id = WorkingSet::INVALID_ID;
                PlanStage::StageState state = scan->work(&id);
                if (PlanStage::ADVANCED == state) {
                    WorkingSetMember* member = ws->get(id);
                    verify(member->hasLoc());
                    out->push_back(member->loc);
                }
            }
        }

        IndexDescriptor* getIndex(const BSONObj& obj, Collection* coll) {
            IndexDescriptor* descriptor =
                coll->getIndexCatalog()->findIndexByKeyPattern( &_txn, obj );
            if (NULL == descriptor) {
                FAIL(mongoutils::str::stream()
                     << "Unable to find index with key pattern " << obj);
            }
            return descriptor;
        }

        static const char* ns() { return "unittests.QueryStageIndexScan"; }

    protected:
        OperationContextImpl _txn;

    private:
        DBDirectClient _client;
    };

    //
    // Run the baseline tests.
    //
    class QueryStageIXScanConcurrencyBaseline : public QueryStageIXScanBase {
    public:
        void run() {
            Client::WriteContext ctx(&_txn, ns());
            Database* db = ctx.db();
            Collection* coll = db->getCollection(&_txn, ns());
            if (!coll) {
                coll = db->createCollection(&_txn, ns());
            }

            setup();

            // set up our index scan
            IndexScanParams params;
            params.descriptor = getIndex(BSON("a" << 1), coll);
            params.bounds.isSimpleRange = true;
            params.bounds.startKey = BSON("" << 50);
            params.bounds.endKey = BSON("" << 100);
            params.bounds.endKeyInclusive = true;
            params.direction = -1;

            WorkingSet ws;
            IndexScan tree(&_txn, params, &ws, NULL);

            // Make a dummy scan and use to get DiskLocs
            WorkingSet wsDummy;
            IndexScan dummy(&_txn, params, &wsDummy, NULL);
            getLocs(&dummy, &wsDummy, &g_locs);

            // make a tree of execution
            BasicConcurrencyTest* baseline =
                new BasicConcurrencyTest(&tree, coll, ns(),
                                            &QueryStageIXScan::produce,
                                            &QueryStageIXScan::getDiskLoc,
                                            &QueryStageIXScan::getNewDocument,
                                            &_txn);
            baseline->test();

            delete baseline;

            ctx.commit();
        }
    };

    class All : public Suite {
    public:
        All() : Suite("query_stage_index_scan") { }

        void setupTests() {
            add<QueryStageIXScanConcurrencyBaseline>();
        }
    } queryStaceIXScanAll;

} // namespace
