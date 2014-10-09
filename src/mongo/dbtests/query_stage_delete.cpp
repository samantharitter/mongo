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

/**
 * This file tests db/exec/delete.cpp.
 */

#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/collection_scan.h"
#include "mongo/db/exec/delete.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/dbtests/dbtests.h"

namespace QueryStageDelete {

    //
    // Stage-specific tests.
    //

    class QueryStageDeleteBase {
    public:
        QueryStageDeleteBase() : _client(&_txn) {
            Client::WriteContext ctx(&_txn, ns());

            for (size_t i = 0; i < numObj(); ++i) {
                BSONObjBuilder bob;
                bob.append("a", static_cast<long long int>(i));
                _client.insert(ns(), bob.obj());
            }
            ctx.commit();
        }

        virtual ~QueryStageDeleteBase() {
            Client::WriteContext ctx(&_txn, ns());
            _client.dropCollection(ns());
            ctx.commit();
        }

        void remove(const BSONObj& obj) {
            _client.remove(ns(), obj);
        }

        void mutate(const BSONObj& oldObj, const BSONObj& newObj) {
            _client.update(ns(), oldObj, newObj);
        }

        Collection* getCollection(Client::WriteContext* ctx) {
            return ctx->ctx().db()->getCollection(&_txn, ns());
        }

        /*
         * Return a forward in-order collection scan for this collection.
         */
        CollectionScan* getCollectionScan(Client::WriteContext* ctx, WorkingSet* ws,
                                          const MatchExpression* query) {
            CollectionScanParams params;
            params.collection = getCollection(ctx);
            params.direction = CollectionScanParams::FORWARD;
            params.tailable = false;
            return new CollectionScan(&_txn, params, ws, query);
        }

        /*
         * Given a PlanStage, exhaust it and collect its disklocs.
         */
        void getLocs(auto_ptr<PlanStage> scan, WorkingSet* ws, vector<DiskLoc>* out) {
            while (!scan->isEOF()) {
                WorkingSetID id = WorkingSet::INVALID_ID;
                PlanStage::StageState state = scan->work(&id);
                if (PlanStage::ADVANCED == state) {
                    WorkingSetMember* member = ws->get(id);
                    verify(member->hasLoc());
                    if (member->hasLoc()) out->push_back(member->loc);
                }
            }
        }

        /*
         * Delete the next 'toDelete' documents using this DeleteStage,
         * or fail.
         */
        void deleteN(DeleteStage* deleteStage, const DeleteStats* stats,
                     const size_t toDelete) {
            while (stats->docsDeleted < toDelete) {
                WorkingSetID id = WorkingSet::INVALID_ID;
                PlanStage::StageState state = deleteStage->work(&id);
                invariant(state == PlanStage::NEED_TIME);
            }
        }

        /*
         * Delete documents using this DeleteStage until we hit EOF or fail.
         */
        void deleteAll(DeleteStage* deleteStage) {
            WorkingSetID id;

            while (!deleteStage->isEOF()) {
                id = WorkingSet::INVALID_ID;
                PlanStage::StageState state = deleteStage->work(&id);
                invariant(PlanStage::NEED_TIME == state || PlanStage::IS_EOF == state);
            }
        }

        static size_t numObj() { return 50; }

        static const char* ns() { return "unittests.QueryStageDelete"; }

    protected:
        OperationContextImpl _txn;

    private:
        DBDirectClient _client;
    };

    //
    // Test invalidation for the delete stage.  Use the delete stage to delete some objects
    // retrieved by a collscan, then invalidate the upcoming object, then expect the delete stage to
    // skip over it and successfully delete the rest.
    //
    class QueryStageDeleteInvalidateUpcomingObject : public QueryStageDeleteBase {
    public:
        void run() {
            Client::WriteContext ctx(&_txn, ns());
            Collection* coll = getCollection(&ctx);

            // Configure a collection scan
            WorkingSet ws;
            CollectionScan* collScanDelete = getCollectionScan(&ctx, &ws, NULL);

            // Create an identical scan and use it to fetch DiskLocs.
            WorkingSet wsDummy;
            auto_ptr<PlanStage> dummy(getCollectionScan(&ctx, &wsDummy, NULL));
            vector<DiskLoc> locs;
            getLocs(dummy, &wsDummy, &locs);

            // Configure our delete stagee
            DeleteStageParams deleteStageParams;
            deleteStageParams.isMulti = true;
            deleteStageParams.shouldCallLogOp = false;
            DeleteStage deleteStage(&_txn, deleteStageParams, &ws, coll, collScanDelete);

            // Configure stats, for tracking
            const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage.getSpecificStats());

            // Delete some documents
            const size_t targetDocIndex = 10;
            deleteN(&deleteStage, stats, targetDocIndex);

            // Prepare to yield
            deleteStage.saveState();

            // Remove and invalidate locs[targetDocIndex]
            deleteStage.invalidate(locs[targetDocIndex], INVALIDATION_DELETION);
            BSONObj targetDoc = coll->docFor(&_txn, locs[targetDocIndex]);
            ASSERT(!targetDoc.isEmpty());
            remove(targetDoc);

            // Restore from yield, remove the rest
            deleteStage.restoreState(&_txn);
            deleteAll(&deleteStage);
            ASSERT_EQUALS(numObj() - 1, stats->docsDeleted);

            ctx.commit();
        }
    };

    //
    // Test that DeleteStage recovers properly from an INVALIDATION_MUTATION of
    // one of its upcoming documents, when the document still matches the query.
    //
    class QueryStageDeleteMutatedUpcomingObject : public QueryStageDeleteBase {
    public:
        void run() {
            Client::WriteContext ctx(&_txn, ns());
            Collection* coll = getCollection(&ctx);

            // Configure a collection scan to run under our DeleteStage
            WorkingSet ws;
            CollectionScan* collScanDelete = getCollectionScan(&ctx, &ws, NULL);

            // Create an identical collection scan and use it to
            // get the DiskLocs that this CollScan will return
            WorkingSet wsDummy;
            auto_ptr<PlanStage> dummy(getCollectionScan(&ctx, &wsDummy, NULL));
            vector<DiskLoc> locs;
            getLocs(dummy, &wsDummy, &locs);

            // Configure our delete stage
            DeleteStageParams deleteStageParams;
            deleteStageParams.isMulti = true;
            deleteStageParams.shouldCallLogOp = false;
            DeleteStage deleteStage(&_txn, deleteStageParams, &ws, coll, collScanDelete);

            // Configure stats, so we can track this delete.
            const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage.getSpecificStats());

            // Delete some documents
            const size_t toDelete = 10;
            deleteN(&deleteStage, stats, toDelete);

            // Prepare to yield
            deleteStage.saveState();

            // Mutate and invalidate next document, it will still match query
            deleteStage.invalidate(locs[toDelete], INVALIDATION_MUTATION);
            BSONObj oldObj = coll->docFor(&_txn, locs[toDelete]);
            ASSERT(!oldObj.isEmpty());
            mutate(oldObj, BSON("b" << "1"));

            // Recover from yield, delete all documents
            deleteStage.restoreState(&_txn);
            deleteAll(&deleteStage);
            ASSERT_EQUALS(numObj(), stats->docsDeleted);

            ctx.commit();
        }
    };

    //
    // Test that DeleteStage recovers properly when an upcoming object is
    // mutated and invalidated such that it no longer matches our query.
    //
    class QueryStageDeleteMutateObjectNoMatch : public QueryStageDeleteBase {
    public:
        void run() {
            Client::WriteContext ctx(&_txn, ns());
            Collection* coll = getCollection(&ctx);

            // Set up a collectionScan for {a : {"$gt" : 5 }}
            BSONObj query = BSON("a" << BSON("$gt" << 5));
            StatusWithMatchExpression status = MatchExpressionParser::parse(query);
            ASSERT(status.isOK());
            MatchExpression* filter(status.getValue());

            WorkingSet ws;
            CollectionScan* filteredScan = getCollectionScan(&ctx, &ws, filter);

            // Duplicate, get diskLocs
            WorkingSet wsDummy;
            auto_ptr<PlanStage> dummy(getCollectionScan(&ctx, &wsDummy, filter));
            vector<DiskLoc> locs;
            getLocs(dummy, &wsDummy, &locs);

            // Set up our DeleteStage
            DeleteStageParams deleteParams;
            deleteParams.isMulti = true;
            deleteParams.shouldCallLogOp = false;
            DeleteStage deleteStage(&_txn, deleteParams, &ws, coll, filteredScan);

            // Configure stats, to track delete
            const DeleteStats* stats = static_cast<const DeleteStats*>(deleteStage.getSpecificStats());

            // Delete some documents
            const size_t toDelete = 10;
            deleteN(&deleteStage, stats, toDelete);

            // Prepare to yield
            deleteStage.saveState();

            // Mutate and invalidate a document so that it does not match query
            deleteStage.invalidate(locs[toDelete], INVALIDATION_MUTATION);
            BSONObj oldObj = coll->docFor(&_txn, locs[toDelete]);
            ASSERT(!oldObj.isEmpty());
            mutate(oldObj, BSON("a" << -1));

            // recover and delete all documents, skipping invalidated one
            deleteStage.restoreState(&_txn);
            deleteAll(&deleteStage);
            ASSERT_EQUALS(numObj() - 7, stats->docsDeleted);

            ctx.commit();
        }
    };

    class All : public Suite {
    public:
        All() : Suite("query_stage_delete") {}

        void setupTests() {
            // Stage-specific tests below.
            add<QueryStageDeleteInvalidateUpcomingObject>();
            add<QueryStageDeleteMutatedUpcomingObject>();
            add<QueryStageDeleteMutateObjectNoMatch>();
        }
    } all;

}
