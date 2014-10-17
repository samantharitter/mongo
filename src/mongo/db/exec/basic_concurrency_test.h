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

#pragma once

#include "mongo/db/dbdirectclient.h"
#include "mongo/db/exec/plan_stage.h"

//
// This file provides a framework for testing stages of execution for basic
// concurrency-related correctness.
//

namespace mongo {

    class BasicConcurrencyTest {
    public:

        //
        // This class includes a set of tests to confirm baseline concurrency-related
        // behavior.  Callers are responsible for seeding a collection themselves
        // with a good number (at least 50) of matching and non-matching documents
        // for their query.
        //
        // After running, tests will attempt to validate the tree's results.  A validation
        // callback function should be provided prior to running tests by using
        // setValidationFn(...).
        //
        // We do not own 'tree', it must outlive this object and
        // be cleaned up by the caller.
        //
        // This class requires several callback functions:
        //
        //    void produce(PlanStage* tree)
        //       - call work() on the given tree until the relevant step
        //         is produced (ex. DeleteStage should be worked until it
        //         deletes a document)
        //
        //    DiskLoc getDiskLoc(PlanStage* tree, int nProduced, bool match)
        //       - given a tree where we have produced 'nProduced' documents,
        //         return a DiskLoc for a document we haven't seen so the test can
        //         mess with its document.  If 'match' is true this document
        //         should match the query, if false, it should be a document
        //         in the collection that doesn't match.
        //
        //    BSONObj getNewDocument(int nProduced, bool match)
        //       - return a document.  If 'match' it should match the query,
        //         and be upcoming in the underlying scan. Otherwise, it
        //         should not match the query.
        //
        BasicConcurrencyTest(PlanStage* tree,
                                Collection* coll,
                                const char* ns,
                                void (*produce)(PlanStage*),
                                DiskLoc (*getDiskLoc)(PlanStage*, int, bool),
                                BSONObj (*getNewDocument)(int, bool),
                                OperationContext* txn);
        virtual ~BasicConcurrencyTest() { /* TODO */ }

        //
        // Given a tree of PlanStages, run the tree through all the concurrency-related
        // sanity tests.  These tests will exhaust the tree of results.
        //
        void test();

    private:

        // test cases
        void noopYieldTest();
        void invalidationDeleteTest();
        void invalidationMutationNoMatchTest();
        void invalidationMutationMatchTest();
        void invalidationDeleteIrrelevant();
        void invalidationMutationNewMatchTest();
        void mixedInvalidationTest();
        void insertionTest();

        // helpers for our invalidations
        void invalidateDelete(bool matchBefore);
        void invalidateMutate(bool matchBefore, bool matchAfter);

        // we don't own the tree, but it must outlive this test.
        PlanStage* _tree;

        // stuff we need
        Collection* _coll;
        const char* _ns;

        // various callback functions
        void (*_produce)(PlanStage*);
        DiskLoc (*_getDiskLoc)(PlanStage*, int, bool);
        BSONObj (*_getNewDocument)(int, bool);

        OperationContext* _txn;
        DBDirectClient _client;

        // for bookkeeping
        int _nProduced;
        int _matchesAdded;
    };
}
