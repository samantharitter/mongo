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
#include "mongo/transport/session.h"
#include "mongo/transport/ticket.h"

namespace mongo {

namespace transport {

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
    virtual ~TransportLayer() = default;

    /**
     * Source (receive) a new Message for this Session.
     *
     * This method returns a work Ticket. The caller must complete the Ticket by
     * passing it to either TransportLayer::wait() or TransportLayer::asyncWait().
     *
     * If the given Session is invalid, the returned Ticket will contain an error
     * status.
     *
     * Upon completion, the returned Ticket will be populated with a status. If the
     * TransportLayer is unable to source a Message, this will be a failed status,
     * and the passed-in Message buffer may be left in an invalid state.
     */
    virtual Ticket sourceMessage(Message& message, Session session) = 0;

    /**
     * Sink (send) a new Message for this Session. This method should be used
     * to send replies to a given host.
     *
     * This method returns a work Ticket. The caller must complete the Ticket by
     * passing it to either TransportLayer::wait() or TransportLayer::asyncWait().
     *
     * If the given Session is invalid, the returned Ticket will contain an error
     * status.
     *
     * Upon completion, the returned Ticket will be populated with a status. If the
     * TransportLayer is unable to sink the given Message, this will be a failed status,
     * and the passed-in Message buffer may be left in an invalid state.
     *
     * This method does NOT take ownership of the sunk Message, which must be cleaned
     * up by the caller.
     */
    virtual Ticket sinkMessage(Message& message, Session session) = 0;

    /**
     * Perform a synchronous wait on the given work Ticket. When this call returns,
     * the Ticket will be populated with the results of its work.
     *
     * This thread may be used by the TransportLayer to run other Tickets that were
     * enqueued prior to this call.
     */
    virtual Status wait(Ticket&& ticket) = 0;

    /**
     * Callback for Tickets that are run via asyncWait().
     */
    using TicketCallback = stdx::function<void(Status)>;

    /**
     * Perform an asynchronous wait on the given work Ticket. Once the Ticket has been
     * completed, the passed-in callback will be invoked.
     *
     * This thread will not be used by the TransportLayer to perform work. The callback
     * passed to asyncWait() may be run on any thread.
     */
    virtual void asyncWait(Ticket&& ticket, TicketCallback callback) = 0;

    /**
     * End the given Session. Future calls to sourceMessage() or sinkMessage()
     * for this Session will fail. Tickets for this Session that have already been
     * started via wait() or asyncWait() will complete, but may return a failed status.
     */
    virtual void end(Session session) = 0;

    /**
     * End all active sessions in the TransportLayer. Tickets that have already been
     * started via wait() or asyncWait() will complete, but may return a failed status.
     */
    virtual void endAllSessions() = 0;

    /**
     * Shut the TransportLayer down. After this point, the TransportLayer will
     * end all active sessions and won't accept new transport::Endpoints. Any
     * future calls to sourceMessage() or sinkMessage() will fail.
     */
    virtual void shutdown() = 0;
};

}  // namespace transport

}  // namespace mongo
