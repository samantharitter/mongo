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

#include "mongo/bson/bsontypes.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/db/client.h"
#include "mongo/db/db.h"
#include "mongo/db/instance.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/util/net/poc.h"
#include "mongo/stdx/chrono.h"

namespace mongo {

    //    extern PortMessageServer* centralServer;

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

    long long PocServer::benchmark(MessageHandler* messageHandler) {
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
        std::cout << _n << " networkless updates took " << dur << " milliseconds\n";

        delete [] queue;

        return dur;
    }

    long long PocServer::benchmarkSocket() {

        // make sure the db starts up
        // this needs to go in another thread
        boost::thread t(initAndListen, 27017);

        PortMessageServer* server = centralServer;
        std::cout << "1\n";
        // connect to ourselves
        const SockAddr addr("127.0.0.1", 27017);
        const SockAddr from("127.0.0.1", 27017);
        int sock = ::socket(addr.getType(), SOCK_STREAM, 0);
        ::bind(sock, addr.raw(), addr.addressSize);
        std::cout << "2\n";
        boost::shared_ptr<Socket> pnewSock( new Socket(sock, from) );
        verify(server);
        server->accepted(pnewSock, 34012); // this forks, right?
        std::cout << "2.75\n";
        // make messages
        Message* queue = new Message[_n];
        for (int i = 0; i < _n; i++) {
            fillMessage(&queue[i]);
        }
        std::cout << "3\n";
        boost::shared_ptr<MessagingPort> mp(new MessagingPort(sock, from));
        auto start = stdx::chrono::high_resolution_clock::now();
        for (int i = 0; i < _n; i++) {
            // send
            Message m = queue[i];
            m.send( *mp, "context" );
        }
        std::cout << "4\n";
        auto end = stdx::chrono::high_resolution_clock::now();
        long long dur = stdx::chrono::duration_cast<stdx::chrono::milliseconds>(end - start).count();
        std::cout << _n << " same socket updates took " << dur << " milliseconds\n";

        //delete [] queue;
        std::cout << "5\n";

        t.join();

        return dur;
    }

    /**
     * This method generates 'n' messages for update operations and
     * sends them up to the database immediately.
     */
    void PocServer::run(MessageHandler* messageHandler) {
        long long x = 0;
        for (int i = 0; i < _count; i++) {
            x += benchmark(messageHandler);
        }
        std::cout << "\n\t\t\t--- Network-less updates ---\n";
        std::cout << "\t\t\tPerformed " << _count << " runs.\n";
        std::cout << "\t\t\tAverage time to run " << _n << " updates: "
                  << x/_count << " milliseconds\n\n";

        x = 0;
        for (int i = 0; i < _count; i++) {
            x += benchmarkSocket();
        }
        std::cout << "\n\t\t\t--- Same socket updates ---\n";
        std::cout << "\t\t\tPerformed " << _count << " runs.\n";
        std::cout << "\t\t\tAverage time to run " << _n << " updates: "
                  << x/_count << " milliseconds\n\n";

    }

} // namespace mongo
