COUNTER=0

# set this for the test name
TESTNAME="jstests/sharding/mongos_rs_shard_failure_tolerance.js"

# set this logging line so we break on error
REPROTEXT="pure virtual"

while true; do
    echo ""
    echo ""
    echo "============================================================="
    echo "Starting run $COUNTER..."
    echo "============================================================="
    echo ""
    echo ""

    sleep 3

    # set a new filename for our output
    FILENAME="out$COUNTER.txt"

    # run the jstest
    python ./buildscripts/resmoke.py --executor=sharding --storageEngine=wiredTiger -j$(kstat cpu | sort -u | grep -c "^module") $TESTNAME | tee $FILENAME
    #ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-3.4 LSAN_OPTIONS="suppressions=etc/lsan.suppressions" ASAN_OPTIONS=detect_leaks=1 ./mongo --eval 'MongoRunner.dataDir = "/data/db/job0/mongorunner"; TestData = new Object(); TestData.wiredTigerEngineConfigString = ""; TestData.maxPort = 20249; TestData.wiredTigerIndexConfigString = ""; TestData.noJournal = false; TestData.testName = "auth"; TestData.storageEngine = "wiredTiger"; TestData.minPort = 20010; TestData.noJournalPrealloc = true; TestData.wiredTigerCollectionConfigString = ""; MongoRunner.dataPath = "/data/db/job0/mongorunner/"' --readMode commands --nodb jstests/sharding/auth.js | tee $FILENAME

    # check if we reproduced the bug
    echo ""
    echo ""
    echo "Did we reproduce the error '$REPROTEXT' ?"
    echo ""

    if grep -q "$REPROTEXT" $FILENAME; then
        echo "Yes! On run $COUNTER. Goodbye."
        echo ""
        exit 1
    fi

    echo "No, trying again."

    let COUNTER+=1

done
