// for networking proof of concept

/*    Copyright 2015 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include <chrono>
#include <thread>

#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/client.h"
#include "mongo/db/db.h"
#include "mongo/db/db_shared.h"
#include "mongo/db/instance.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/util/net/poc.h"
#include "mongo/stdx/chrono.h"

namespace mongo {

    void PocMessageHandler::process(Message& m,
                                    AbstractMessagingPort* port,
                                    LastError* le) {
        OperationContextImpl txn;
        DbResponse dbresponse;

        pocAssembleResponse(&txn, m, dbresponse);
    }

    /**
     * Creates a new 'server' for testing a 'network-less' mongod.
     */
    PocServer::PocServer(int n, int count) : _n(n), _count(count) {}

    /**
     * Creates a message for an update operation
     */
    void PocServer::fillMessage(Message* m) {
        // build our "batch" of a single update
        boost::scoped_ptr<BSONArrayBuilder> batch(new BSONArrayBuilder);
        BSONObj selector = fromjson("{ a : 1 }");
        BSONObj update = fromjson("{ $inc : { a : 0 } }");
        BSONObjBuilder updateBuilder;
        updateBuilder.append("q", selector);
        updateBuilder.append("u", update);
        updateBuilder.append("multi", false);
        updateBuilder.append("upsert", false);
        batch->append(updateBuilder.obj());

        // build our command
        boost::scoped_ptr<BSONObjBuilder> command(new BSONObjBuilder);
        command->append("update", "poc"); // command key, collection name
        command->append("updates", batch->arr());
        command->append("ordered", false);

        // build a proper query out of the command
        BufBuilder b;
        b.appendNum(0); // options
        b.appendStr("test.poc"); // ns
        b.appendNum(0); // nToSkip
        b.appendNum(0); // nToReturn
        command->obj().appendSelfToBufBuilder(b); // query

        m->reset();
        m->setData(dbQuery, b.buf(), b.len());
        m->header().setId(nextMessageId());
        m->header().setResponseTo(0);
    }

    long long PocServer::runNetworklessTests(MessageHandler* messageHandler) {
        Message* queue = new Message[_n];

        for (int i = 0; i < _n; i++) {
            fillMessage(&queue[i]);
        }

        // network-less
        auto start = stdx::chrono::high_resolution_clock::now();
        for (int i = 0; i < _n; i++) {
            messageHandler->process(queue[i], NULL, NULL);
        }
        auto end = stdx::chrono::high_resolution_clock::now();
        long long dur = stdx::chrono::duration_cast<stdx::chrono::milliseconds>(end - start).count();
        //std::cout << _n << " networkless updates took " << dur << " milliseconds\n";

        delete [] queue;

        return dur;
    }

    // must call initAndListen before using this
    long long PocServer::runFakeNetworkTests(int port) {

        doneProcessingAll = false;

        // make a tcp socket
        int sock = socket(AF_INET, SOCK_STREAM, 0);
        int success = false;
        SockAddr addr("localhost", port);

        std::cout << "Attempting to connect on port %d...\n, port";
        while (!success) {
            // connect it on an ephemeral port until it succeeds and connects to itself
            int res = ::connect(sock, addr.raw(), addr.addressSize);
            if (res >= 0) {
                success = true;
                std::cout << "Connection made!!\n";
            } else {
                //std::cout << "Failed to connect.\n";
            }
        }

        // wrap it up in Socket class
        boost::shared_ptr<Socket> psocket( new Socket(sock, addr) );
        long long connectionId = 12345;

        // pass this socket to listener
        centralServer->accepted(psocket, connectionId);

        // make messages
        std::cout << "socket accepted, making messages\n";
        Message* queue = new Message[_n];
        for (int i = 0; i < _n; i++) {
            fillMessage(&queue[i]);
        }

        boost::shared_ptr<MessagingPort> mp(new MessagingPort(sock, addr));

        auto time_start = stdx::chrono::high_resolution_clock::now();
        std::cout << "sending messages...\n";
        for (int i = 0; i < _n; i++) {
            // send
            doneProcessing = false;
            Message m = queue[i];
            m.send( *mp, "context" );
        }

        while (!doneProcessingAll) {
            std::this_thread::sleep_for(std::chrono::milliseconds(1000));
        }

        auto time_end = stdx::chrono::high_resolution_clock::now();
        long long dur = stdx::chrono::duration_cast<stdx::chrono::milliseconds>(time_end - time_start).count();
        std::cout << _n << " same socket updates took " << dur << " milliseconds\n";

        psocket->close();

        delete [] queue;

        return dur;
    }

    /**
     * This method generates 'n' messages for update operations and
     * sends them up to the database immediately.
     */
    void PocServer::run(MessageHandler* messageHandler) {
        long long x = 0;
        long long y = 0;

        std::cout << "\n\t\tRunning networkless tests...\n\n";

        for (int i = 0; i < _count; i++) {
            x += runNetworklessTests(messageHandler);
        }

        std::cout << "\n\t\tRunning fake network tests...\n\n";

        // start up the db
        // the "regular" network layer will listen on the first port (27017)
        // the "ASIO" network layer will listen on the second port (27016)
        std::cout << "Calling initAndListenShared\n";
        batchSize = _n;
        boost::thread t(initAndListenShared, 27017);

        for (int i = 0; i < _count; i++) {
            y += runFakeNetworkTests(32768 + i);
        }

        std::cout << "\t\t\tFINAL RESULTS:\n\n";

        std::cout << "\n\t\t\t--- Network-less updates ---\n";
        std::cout << "\t\t\tPerformed " << _count << " runs.\n";
        std::cout << "\t\t\tAverage time to run " << _n << " updates: "
                  << x/_count << " milliseconds\n\n";

        std::cout << "\n\t\t\t--- Same socket updates ---\n";
        std::cout << "\t\t\tPerformed " << _count << " runs.\n";
        std::cout << "\t\t\tAverage time to run " << _n << " updates: "
                  << y/_count << " milliseconds\n\n";

        // todo: force child thread to die
        //t.join();
    }

} // namespace mongo
