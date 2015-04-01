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
#include "mongo/db/instance.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/util/net/poc.h"

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
    PocServer::PocServer(int n) : _n(n) {}

    /**
     * This method generates 'n' messages for update operations and
     * sends them up to the database immediately.
     */
    void PocServer::run(MessageHandler* messageHandler) {
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

        for (int i = 0; i < _n; i++) {
            Message m;
            m.reset();
            m.setData(dbQuery, b.buf(), b.len());
            m.header().setId(nextMessageId());
            m.header().setResponseTo(0);

            messageHandler->process(m, NULL, NULL);
        }
    }

} // namespace mongo
