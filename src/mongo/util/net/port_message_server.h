// port_message_server.h

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

#pragma once

#include "mongo/platform/basic.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/message_server.h"

namespace mongo {

   extern bool doneProcessing;
   extern bool doneProcessingAll;
   extern int batchSize;

    class PortMessageServer : public MessageServer , public Listener {
    public:
        PortMessageServer(  const MessageServer::Options& opts, MessageHandler * handler );
        virtual void accepted(boost::shared_ptr<Socket> psocket, long long connectionId );
        virtual void setAsTimeTracker();
        virtual void setupSockets();
        void run();
        virtual bool useUnixSockets() const { return true; }

    private:
        MessageHandler* _handler;

        static void* handleIncomingMsg(void* arg);
    };

    PortMessageServer* createPortMessageServer( const MessageServer::Options& opts , MessageHandler * handler );

} // namespace mongo
