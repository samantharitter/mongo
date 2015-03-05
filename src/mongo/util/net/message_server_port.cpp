// message_server_port.cpp

/*    Copyright 2009 10gen Inc.
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

#include "mongo/platform/basic.h"

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <memory>

#include "mongo/base/disallow_copying.h"
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
#include "mongo/util/net/listen.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/scopeguard.h"

#include <sys/socket.h>
#include <netinet/in.h>

#ifdef __linux__  // TODO: consider making this ifndef _WIN32
# include <sys/resource.h>
#endif

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

namespace mongo {

    using boost::scoped_ptr;
    using std::endl;

namespace {

    class MessagingPortWithHandler : public MessagingPort {
        MONGO_DISALLOW_COPYING(MessagingPortWithHandler);

    public:
        MessagingPortWithHandler(const boost::shared_ptr<Socket>& socket,
                                 MessageHandler* handler,
                                 long long connectionId)
            : MessagingPort(socket), _handler(handler) {
            setConnectionId(connectionId);
        }

        MessageHandler* getHandler() const { return _handler; }

    private:
        // Not owned.
        MessageHandler* const _handler;
    };

}  // namespace

    class PortMessageServer : public MessageServer , public Listener {
    public:
        /**
         * Creates a new message server.
         *
         * @param opts
         * @param handler the handler to use. Caller is responsible for managing this object
         *     and should make sure that it lives longer than this server.
         */
        PortMessageServer(  const MessageServer::Options& opts, MessageHandler * handler ) :
            Listener( "" , opts.ipList, opts.port ), _handler(handler) {
        }

        virtual void accepted(boost::shared_ptr<Socket> psocket, long long connectionId ) {
            std::auto_ptr<MessagingPortWithHandler> portWithHandler(
                new MessagingPortWithHandler(psocket, _handler, connectionId));

            // send to handleIncomingMsg, which will never return.
            handleIncomingMsg( portWithHandler.get() );
        }

        virtual void setAsTimeTracker() {
            Listener::setAsTimeTracker();
        }

        virtual void setupSockets() {
            Listener::setupSockets();
        }

        void run() {
            initAndListen();
        }

        virtual bool useUnixSockets() const { return true; }

    private:
        MessageHandler* _handler;

        /**
         * Handles incoming messages from a given socket.
         *
         * Terminating conditions:
         * 1. Assertions while handling the request.
         * 2. Socket is closed.
         * 3. Server is shutting down (based on inShutdown)
         *
         * @param arg this method is in charge of cleaning up the arg object.
         *
         * @return NULL
         */
        static void* handleIncomingMsg(void* arg) {
            struct sockaddr_in remote;
            socklen_t addrlen = sizeof(remote);
            unsigned char buf [100];
            int recvlen;

            invariant(arg);

            scoped_ptr<MessagingPortWithHandler> portWithHandler(static_cast<MessagingPortWithHandler*>(arg));
            //MessageHandler* const handler = portWithHandler->getHandler();

            for (;;) {
                recvlen = recvfrom(portWithHandler->psock->rawFD(), buf, 100, 0,
                                   (struct sockaddr *)&remote, &addrlen);
                if (recvlen > 0) {
                    buf[recvlen] = 0;
                    std::cout << "SERVER: " << buf << std::endl;
                }
            }
        }
    };


    MessageServer * createServer( const MessageServer::Options& opts , MessageHandler * handler ) {
        return new PortMessageServer( opts , handler );
    }

}  // namespace mongo
