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

#include "mongo/platform/basic.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/exec/basic_concurrency_test.h"

namespace mongo {

    typedef void (BasicConcurrencyTest::*BaseTest)();

    BasicConcurrencyTest::BasicConcurrencyTest(PlanStage* tree,
                                               Collection* coll,
                                               const char* ns,
                                               void (*produce)(PlanStage*),
                                               DiskLoc (*getDiskLoc)(PlanStage*, int, bool),
                                               BSONObj (*getNewDocument)(int, bool),
                                               OperationContext* txn)
        : _tree(tree),
        _coll(coll),
        _ns(ns),
        _produce(produce),
        _getDiskLoc(getDiskLoc),
        _getNewDocument(getNewDocument),
        _txn(txn),
        _client(_txn),
        _nProduced(0),
        _matchesAdded(0)
    { }

    void BasicConcurrencyTest::test() {
        BaseTest tests[] = {
            &BasicConcurrencyTest::noopYieldTest,
            &BasicConcurrencyTest::invalidationDeleteTest,
            &BasicConcurrencyTest::invalidationDeleteIrrelevant,
            &BasicConcurrencyTest::invalidationMutationNoMatchTest,
            &BasicConcurrencyTest::invalidationMutationMatchTest,
            &BasicConcurrencyTest::invalidationMutationNewMatchTest,
            &BasicConcurrencyTest::mixedInvalidationTest,
            &BasicConcurrencyTest::insertionTest
        };

        for (size_t i = 0; i < (sizeof(tests)/sizeof(tests[0])); i++) {
            if (_tree->isEOF()) return;

            // do some work
            _produce(_tree);
            ++_nProduced;

            // yield
            _tree->saveState();

            // run our test-specific interjection
            (this->*(tests[i]))();

            // recover
            _tree->restoreState(_txn);

            // do some more work
            _produce(_tree);
            ++_nProduced;
        };
    }

    void BasicConcurrencyTest::invalidateDelete(bool matchBefore) {
        DiskLoc dl = _getDiskLoc(_tree, _nProduced, matchBefore);
        BSONObj doc = _coll->docFor(_txn, dl);
        _client.remove(_ns, doc);
        _tree->invalidate(dl, INVALIDATION_DELETION);
        if (matchBefore) --_matchesAdded;
    }

    void BasicConcurrencyTest::invalidateMutate(bool matchBefore, bool matchAfter) {
        DiskLoc dl = _getDiskLoc(_tree, _nProduced, matchBefore);
        BSONObj oldDoc = _coll->docFor(_txn, dl);
        BSONObj newDoc = _getNewDocument(_nProduced, matchAfter);
        _client.update(_ns, oldDoc, newDoc);
        _tree->invalidate(dl, INVALIDATION_MUTATION);
        if (matchBefore && !matchAfter) --_matchesAdded;
        else if (!matchBefore && matchAfter) ++_matchesAdded;
    }

    void BasicConcurrencyTest::noopYieldTest() {
        // no-op yield
        return;
    }

    void BasicConcurrencyTest::invalidationDeleteTest() {
        invalidateDelete(true);
    }

    void BasicConcurrencyTest::invalidationDeleteIrrelevant() {
        invalidateDelete(false);
    }

    void BasicConcurrencyTest::invalidationMutationNoMatchTest() {
        // mutate a document so it no longer matches
        invalidateMutate(true, false);
    }

    void BasicConcurrencyTest::invalidationMutationMatchTest() {
        // mutate a document so it still matches
        invalidateMutate(true, true);
    }

    void BasicConcurrencyTest::invalidationMutationNewMatchTest() {
        // update a previously-non-matching doc so it matches
        invalidateMutate(false, true);
    }

    void BasicConcurrencyTest::mixedInvalidationTest() {
        // remove several documents, both matching and non-matching
        invalidateDelete(false);
        invalidateDelete(true);
        invalidateDelete(true);
        invalidateDelete(false);
        invalidateDelete(false);

        // do several updates
        invalidateMutate(true, true);
        invalidateMutate(false, false);
        invalidateMutate(true, false);
        invalidateMutate(true, true);
        invalidateMutate(true, true);

        // update a document so it doesn't match, then remove it
        DiskLoc dl = _getDiskLoc(_tree, _nProduced, true);
        BSONObj oldDoc = _coll->docFor(_txn, dl);
        BSONObj newDoc = _getNewDocument(_nProduced, false);
        _client.update(_ns, oldDoc, newDoc);
        _tree->invalidate(dl, INVALIDATION_MUTATION);
        _client.remove(_ns, newDoc);
        _tree->invalidate(dl, INVALIDATION_DELETION);

        // update a document so it still matches, then remove it
        dl = _getDiskLoc(_tree, _nProduced, true);
        oldDoc = _coll->docFor(_txn, dl);
        newDoc = _getNewDocument(_nProduced, true);
        _client.update(_ns, oldDoc, newDoc);
        _tree->invalidate(dl, INVALIDATION_MUTATION);
        _client.remove(_ns, newDoc);
        _tree->invalidate(dl, INVALIDATION_DELETION);
    }

    void BasicConcurrencyTest::insertionTest() {
        // insert some things
        _client.insert(_ns, _getNewDocument(_nProduced, true));
        _client.insert(_ns, _getNewDocument(_nProduced, true));
        _client.insert(_ns, _getNewDocument(_nProduced, true));
        _client.insert(_ns, _getNewDocument(_nProduced, true));
        _client.insert(_ns, _getNewDocument(_nProduced, true));
        _client.insert(_ns, _getNewDocument(_nProduced, false));
        _client.insert(_ns, _getNewDocument(_nProduced, false));
    }

} // namespace
