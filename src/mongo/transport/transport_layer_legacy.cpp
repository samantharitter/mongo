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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kNetwork

#include "mongo/platform/basic.h"

#include "mongo/transport/transport_layer_legacy.h"

#include "mongo/base/checked_cast.h"
#include "mongo/config.h"
#include "mongo/db/service_context.h"
#include "mongo/db/stats/counters.h"
#include "mongo/stdx/functional.h"
#include "mongo/stdx/memory.h"
#include "mongo/transport/service_entry_point.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/log.h"
#include "mongo/util/net/abstract_message_port.h"
#include "mongo/util/net/socket_exception.h"

namespace mongo {
namespace transport {

TransportLayerLegacy::ListenerLegacy::ListenerLegacy(const TransportLayerLegacy::Options& opts,
                                                     NewConnectionCb callback)
    : Listener("", opts.ipList, opts.port, getGlobalServiceContext(), true),
      _accepted(std::move(callback)) {}

void TransportLayerLegacy::ListenerLegacy::accepted(std::unique_ptr<AbstractMessagingPort> mp) {
    _accepted(std::move(mp));
}

TransportLayerLegacy::TransportLayerLegacy(const TransportLayerLegacy::Options& opts,
                                           ServiceEntryPoint* sep)
    : _sep(sep),
      _listener(stdx::make_unique<ListenerLegacy>(
          opts,
          stdx::bind(&TransportLayerLegacy::_handleNewConnection, this, stdx::placeholders::_1))),
      _running(false),
      _options(opts) {}

TransportLayerLegacy::LegacySession::LegacySession(HostAndPort remote,
                                                   HostAndPort local,
                                                   TransportLayerLegacy* tl)
    : _id(0), _remote(remote), _local(local), _tl(tl) {}

TransportLayerLegacy::LegacySession::~LegacySession() {
    if (_tl != nullptr) {
        _tl->_destroy(this);
    }
}

TransportLayerLegacy::LegacySession::LegacySession(LegacySession&& other)
    : _remote(std::move(other._remote)),
      _local(std::move(other._local)),
      _tags(other._tags),
      _tl(other._tl),
      _connection(other._connection) {
    // We do not want to call tl->destroy() on moved-from LegacySessions.
    other._tl = nullptr;
}

TransportLayerLegacy::LegacySession& TransportLayerLegacy::LegacySession::operator=(
    LegacySession&& other) {
    if (&other == this) {
        return *this;
    }

    _remote = std::move(other._remote);
    _local = std::move(other._local);
    _tags = other._tags;
    _tl = other._tl;
    _connection = other._connection;

    // We do not want to call tl->destroy() on moved-from LegacySessions.
    other._tl = nullptr;

    return *this;
}

Session::TagMask TransportLayerLegacy::LegacySession::getTags() const {
    return _tags;
}

void TransportLayerLegacy::LegacySession::replaceTags(TagMask tags) {
    _tags = tags;
}

const HostAndPort& TransportLayerLegacy::LegacySession::local() const {
    return _local;
}

const HostAndPort& TransportLayerLegacy::LegacySession::remote() const {
    return _remote;
}

SSLPeerInfo TransportLayerLegacy::LegacySession::getX509PeerInfo() const {
    return _connection->sslPeerInfo.value_or(SSLPeerInfo());
}

TransportLayer* TransportLayerLegacy::LegacySession::getTransportLayer() const {
    return _tl;
}

MessageCompressorManager& TransportLayerLegacy::LegacySession::getCompressorManager() {
    return _messageCompressorManager;
}

TransportLayerLegacy::LegacyTicket::LegacyTicket(const SessionHandle& session,
                                                 Date_t expiration,
                                                 WorkHandle work)
    : _session(session),
      _sessionId(session->id()),
      _expiration(expiration),
      _fill(std::move(work)) {}

Session::Id TransportLayerLegacy::LegacyTicket::sessionId() const {
    return _sessionId;
}

Date_t TransportLayerLegacy::LegacyTicket::expiration() const {
    return _expiration;
}

Status TransportLayerLegacy::setup() {
    if (!_listener->setupSockets()) {
        error() << "Failed to set up sockets during startup.";
        return {ErrorCodes::InternalError, "Failed to set up sockets"};
    }

    return Status::OK();
}

Status TransportLayerLegacy::start() {
    if (_running.swap(true)) {
        return {ErrorCodes::InternalError, "TransportLayer is already running"};
    }

    _listenerThread = stdx::thread([this]() { _listener->initAndListen(); });

    return Status::OK();
}

TransportLayerLegacy::~TransportLayerLegacy() = default;

Ticket TransportLayerLegacy::sourceMessage(const SessionHandle& session,
                                           Message* message,
                                           Date_t expiration) {
    auto& compressorMgr = session->getCompressorManager();
    auto sourceCb = [message, &compressorMgr](AbstractMessagingPort* amp) -> Status {
        if (!amp->recv(*message)) {
            return {ErrorCodes::HostUnreachable, "Recv failed"};
        }

        networkCounter.hitPhysical(message->size(), 0);
        if (message->operation() == dbCompressed) {
            auto swm = compressorMgr.decompressMessage(*message);
            if (!swm.isOK())
                return swm.getStatus();
            *message = swm.getValue();
        }
        networkCounter.hitLogical(message->size(), 0);
        return Status::OK();
    };

    return Ticket(this, stdx::make_unique<LegacyTicket>(session, expiration, std::move(sourceCb)));
}

SSLPeerInfo TransportLayerLegacy::getX509PeerInfo(const SessionHandle& session) const {
    LegacySession* impl = checked_cast<LegacySession*>(session->impl());
    return impl->_connection->sslPeerInfo.value_or(SSLPeerInfo());
}

TransportLayer::Stats TransportLayerLegacy::sessionStats() {
    Stats stats;
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        stats.numOpenSessions = _connections.size();
    }

    stats.numAvailableSessions = Listener::globalTicketHolder.available();
    stats.numCreatedSessions = Listener::globalConnectionNumber.load();

    return stats;
}

Ticket TransportLayerLegacy::sinkMessage(const SessionHandle& session,
                                         const Message& message,
                                         Date_t expiration) {
    auto& compressorMgr = session->getCompressorManager();
    auto sinkCb = [&message, &compressorMgr](AbstractMessagingPort* amp) -> Status {
        try {
            networkCounter.hitLogical(0, message.size());
            auto swm = compressorMgr.compressMessage(message);
            if (!swm.isOK())
                return swm.getStatus();
            const auto& compressedMessage = swm.getValue();
            amp->say(compressedMessage);
            networkCounter.hitPhysical(0, compressedMessage.size());

            return Status::OK();
        } catch (const SocketException& e) {
            return {ErrorCodes::HostUnreachable, e.what()};
        }
    };

    return Ticket(this, stdx::make_unique<LegacyTicket>(session, expiration, std::move(sinkCb)));
}

Status TransportLayerLegacy::wait(Ticket&& ticket) {
    return _runTicket(std::move(ticket));
}

void TransportLayerLegacy::asyncWait(Ticket&& ticket, TicketCallback callback) {
    // Left unimplemented because there is no reasonable way to offer general async waiting besides
    // offering a background thread that can handle waits for multiple tickets. We may never
    // implement this for the legacy TL.
    MONGO_UNREACHABLE;
}

void TransportLayerLegacy::end(const SessionHandle& session) {
    LegacySession* impl = checked_cast<LegacySession*>(session->impl());
    _closeConnection(impl->_connection);
}

void TransportLayerLegacy::registerTags(const SessionHandle& session) {
    LegacySession* impl = checked_cast<LegacySession*>(session->impl());
    impl->_connection->tags = session->getTags();
}

void TransportLayerLegacy::_closeConnection(Connection* conn) {
    // NOTE: this method will end communication over a given Connection, but
    // it will not destroy the corresponding Connection object or remove it
    // from _connections.
    conn->closed = true;
    conn->amp->shutdown();
    Listener::globalTicketHolder.release();
}

void TransportLayerLegacy::endAllSessions(Session::TagMask tags) {
    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto&& conn = _connections.begin();
        while (conn != _connections.end()) {
            if (conn->second.tags & tags) {
                log() << "Skip closing connection for connection # " << conn->second.connectionId;
            } else {
                _closeConnection(&(conn->second));
            }

            conn++;
        }
    }
}

void TransportLayerLegacy::shutdown() {
    _running.store(false);
    _listener->shutdown();
    _listenerThread.join();
    endAllSessions(Session::kEmptyTagMask);
}

void TransportLayerLegacy::_destroy(LegacySession* session) {
    // We can only get here from Session's dtor. Since the Session
    // is ending, it is safe to remove and destroy its Connection.
    stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
    auto conn = _connections.find(session->_id);

    invariant(conn != _connections.end());
    if (!conn->second.closed) {
        _closeConnection(&(conn->second));
    }

    _connections.erase(conn);
}

Status TransportLayerLegacy::_runTicket(Ticket ticket) {
    if (!_running.load()) {
        return TransportLayer::ShutdownStatus;
    }

    if (ticket.expiration() < Date_t::now()) {
        return Ticket::ExpiredStatus;
    }

    // Attempt to turn the session weak_ptr into a shared_ptr
    auto legacyTicket = checked_cast<LegacyTicket*>(getTicketImpl(ticket));
    std::shared_ptr<Session> session = legacyTicket->_session.lock();
    if (!session) {
        // Error if we no longer have this session.
        return TransportLayer::TicketSessionClosedStatus;
    }

    // Now that we have the session's shared_ptr, we are guaranteed that the
    // Connection object won't be removed out from under us, because the lifetime
    // of a Connection is tied to its Session's destructor.
    LegacySession* impl = checked_cast<LegacySession*>(session->impl());
    Connection* conn = impl->_connection;
    if (conn->closed) {
        return TransportLayer::TicketSessionClosedStatus;
    }

    // NOTE: no two Tickets should ever be running concurrently for the same Session.
    AbstractMessagingPort* amp = conn->amp.get();

    Status res = Status::OK();

    try {
        res = legacyTicket->_fill(amp);
    } catch (...) {
        res = exceptionToStatus();
    }

#ifdef MONGO_CONFIG_SSL
    // If we didn't have an X509 subject name, see if we have one now
    if (!conn->sslPeerInfo) {
        auto info = amp->getX509PeerInfo();
        if (info.subjectName != "") {
            conn->sslPeerInfo = info;
        }
    }
#endif

    return res;
}

void TransportLayerLegacy::_handleNewConnection(std::unique_ptr<AbstractMessagingPort> amp) {
    if (!Listener::globalTicketHolder.tryAcquire()) {
        log() << "connection refused because too many open connections: "
              << Listener::globalTicketHolder.used();
        amp->shutdown();
        return;
    }

    auto sessionImpl = stdx::make_unique<LegacySession>(
        amp->remote(), HostAndPort(amp->localAddr().toString(true)), this);
    auto session = std::make_shared<Session>(std::move(sessionImpl));

    amp->setLogLevel(logger::LogSeverity::Debug(1));

    {
        stdx::lock_guard<stdx::mutex> lk(_connectionsMutex);
        auto conn =
            _connections.emplace(std::piecewise_construct,
                                 std::forward_as_tuple(session->id()),
                                 std::forward_as_tuple(std::move(amp), false, session->getTags()));

        invariant(conn.second);

        // Set our session impl's connection hook and id
        LegacySession* impl = checked_cast<LegacySession*>(session->impl());
        impl->_connection = &(conn.first->second);
        impl->_connection->closed = false;
        impl->_id = session->id();
    }

    invariant(_sep);
    _sep->startSession(session);
}

}  // namespace transport
}  // namespace mongo
