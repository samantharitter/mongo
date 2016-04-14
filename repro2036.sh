COUNTER=0

# set this for the test name
TESTNAME="jstests/slow1/mr_during_migrate.js"

# set this logging line so we break on error
REPROTEXT="Could not find AsyncOp in _inProgress"

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
    python ./buildscripts/resmoke.py --executor=slow1 --dbpathPrefix=data --storageEngine=wiredTiger -j12 $TESTNAME | tee $FILENAME

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
