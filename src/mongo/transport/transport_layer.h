/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#pragma once

#include "mongo/base/status.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/session.h"

namespace mongo {

/**
 * The TransportLayer moves Messages between transport::Endpoints and the database.
 * This class owns an Acceptor that generates new endpoints from which it can
 * source Messages.
 *
 * The TransportLayer creates Session objects and maps them internally to
 * endpoints. New Sessions are passed to the database (via a ServiceEntryPoint)
 * to be run. The database must then call additional methods on the TransportLayer
 * to manage the Session in a get-Message, handle-Message, return-Message cycle.
 * It must do this on its own thread(s).
 *
 * References to the TransportLayer should be stored on service context objects.
 */
class TransportLayer {
    MONGO_DISALLOW_COPYING(TransportLayer);

public:
    /**
     * Construct a new TransportLayer.
     */
    TransportLayer();

    /**
     * Source (receive) a new Message for this Session. The new Message is placed in
     * the given Message buffer.
     *
     * This method may be implemented in a blocking or non-blocking fashion. In either
     * case, it returns a Deferred which may be immediately populated (if blocking) or
     * populated in the future (if non-blocking). Callers should assume that this
     * method may block.
     *
     * If the TransportLayer is unable to source a Message, it will return a failed
     * status, and the passed-in Message buffer may be left in an invalid state.
     */
    Deferred<Status> sourceMessage(Message& message, Session session);

    /**
     * Sink (send) a new Message for this Session. This method should be used
     * to send replies to a given host.
     *
     * This method may be implemented in a blocking or non-blocking fashion. Callers
     * should assume that this method may block.
     *
     * This method does NOT take ownership of the sunk Message.
     */
    void sinkMessage(Message& message, Session session);

    /**
     * End the given Session. Any future calls to sourceMessage() or sinkMessage()
     * for this Session will fail.
     */
    void end(Session session);

    /**
     * End all active sessions in the TransportLayer.
     */
    void endAllSessions();

    /**
     * Shut the TransportLayer down. After this point, the TransportLayer will
     * end all active sessions and won't accept new transport::Endpoints. Any
     * future calls to sourceMessage() or sinkMessage() will fail.
     */
    void shutdown();
};

}  // namespace mongo
