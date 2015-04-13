// dummy_message_server.cpp

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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/config.h"

#include "mongo/platform/basic.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <memory>

#include "mongo/base/disallow_copying.h"
//#include "mongo/db/db_shared.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/server_options.h"
#include "mongo/db/stats/counters.h"
#include "mongo/stdx/functional.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/net/dummy_message_server.h"
#include "mongo/util/net/port_message_server.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/scopeguard.h"

namespace mongo {

    using boost::scoped_ptr;
    using std::endl;

    /**
     * Creates a new dumb message server that does nothing.
     *
     * @param opts
     * @param handler the handler to use. Caller is responsible for managing this object
     * and should make sure that it lives longer than this server.
     */
    DummyMessageServer::DummyMessageServer( const MessageServer::Options& opts,
                                          MessageHandler * handler ) :
        Listener( "" , opts.ipList, opts.port ), _handler(handler) {}

    void DummyMessageServer::accepted(boost::shared_ptr<Socket> psocket, long long connectionId ) {
        return;
    }

    void DummyMessageServer::setAsTimeTracker() {
        return;
    }

    void DummyMessageServer::setupSockets() {
        return;
    }

    void DummyMessageServer::run() {
        while (true) {
            sleep(1);
        }
    }

    void* DummyMessageServer::handleIncomingMsg(void* arg) {
        return arg;
    }

    DummyMessageServer * createDummyMessageServer( const MessageServer::Options& opts , MessageHandler * handler ) {
        return new DummyMessageServer( opts , handler );
    }

} // namespace mongo
