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

#include "mongo/platform/basic.h"

#include "mongo/transport/transport_layer_manager.h"

#include "mongo/base/status.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/session.h"
#include "mongo/util/time_support.h"

namespace mongo {
namespace transport {

Session::SessionId TransportLayerManager::ManagerTicket::sessionId() const {
    return _ticket.sessionId();
}

Date_t TransportLayerManager::ManagerTicket::expiration() const {
    return _ticket.expiration();
}

TransportLayerManager::ManagerTicket::ManagerTicket(const Session& session, Ticket ticket)
    : _ticket(std::move(ticket)), _sessionTL(session.getTransportLayer()) {}

TransportLayerManager::TransportLayerManager() = default;

Ticket TransportLayerManager::sourceMessage(const Session& session,
                                            Message* message,
                                            Date_t expiration) {
    auto ticket = session.getTransportLayer()->sourceMessage(session, message, expiration);
    return Ticket(this, stdx::make_unique<ManagerTicket>(session, std::move(ticket)));
}

Ticket TransportLayerManager::sinkMessage(const Session& session,
                                          const Message& message,
                                          Date_t expiration) {
    auto ticket = session.getTransportLayer()->sinkMessage(session, message, expiration);
    return Ticket(this, stdx::make_unique<ManagerTicket>(session, std::move(ticket)));
}

Status TransportLayerManager::wait(Ticket ticket) {
    auto mt = dynamic_cast<ManagerTicket*>(getTicketImpl(ticket));
    return mt->_sessionTL->wait(std::move(mt->_ticket));
}

void TransportLayerManager::asyncWait(Ticket ticket, TicketCallback callback) {
    auto mt = dynamic_cast<ManagerTicket*>(getTicketImpl(ticket));
    mt->_sessionTL->asyncWait(std::move(mt->_ticket), std::move(callback));
}

std::string TransportLayerManager::getX509SubjectName(const Session& session) {
    return session.getX509SubjectName();
}

template <typename Callable>
void TransportLayerManager::_foreach(Callable&& cb) {
    {
        stdx::lock_guard<stdx::mutex> lk(_tlsMutex);
        for (auto&& tl : _tls) {
            cb(tl.get());
        }
    }
}

template <typename Callable>
int TransportLayerManager::_sum(Callable&& cb) {
    int sum = 0;
    _foreach([&sum, &cb](TransportLayer* tl) { sum += cb(tl); });
    return sum;
}

int TransportLayerManager::numOpenSessions() {
    return _sum([](TransportLayer* tl) { return tl->numOpenSessions(); });
}

int TransportLayerManager::numAvailableSessions() {
    return _sum([](TransportLayer* tl) { return tl->numAvailableSessions(); });
}

int TransportLayerManager::numCreatedSessions() {
    return _sum([](TransportLayer* tl) { return tl->numCreatedSessions(); });
}

void TransportLayerManager::registerTags(const Session& session) {
    session.getTransportLayer()->registerTags(session);
}

void TransportLayerManager::end(const Session& session) {
    session.getTransportLayer()->end(session);
}

void TransportLayerManager::endAllSessions(Session::TagMask tags) {
    _foreach([&tags](TransportLayer* tl) { tl->endAllSessions(tags); });
}

void TransportLayerManager::shutdown() {
    _foreach([](TransportLayer* tl) { tl->shutdown(); });
}

void TransportLayerManager::addTransportLayer(std::unique_ptr<TransportLayer> tl) {
    {
        stdx::lock_guard<stdx::mutex> lk(_tlsMutex);
        _tls.insert(std::move(tl));
    }
}

}  // namespace transport
}  // namespace mongo
