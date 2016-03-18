COUNTER=0

# set this for the test name
TESTNAME="jstests/replsets/sync_passive.js"

# set this logging line so we break on error
REPROTEXT="Invariant failure now() >= when"

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
    ASAN_SYMBOLIZER_PATH=/usr/bin/llvm-symbolizer-3.4 LSAN_OPTIONS="suppressions=etc/lsan.suppressions" ASAN_OPTIONS=detect_leaks=1 python ./buildscripts/resmoke.py -j$(grep -c ^processor /proc/cpuinfo) $TESTNAME | tee $FILENAME

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
