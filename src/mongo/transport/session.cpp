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

#include "mongo/transport/session.h"

#include "mongo/platform/atomic_word.h"
#include "mongo/transport/transport_layer.h"
#include "mongo/util/net/ssl_types.h"

namespace mongo {
namespace transport {

namespace {

AtomicUInt64 sessionIdCounter(0);

}  // namespace

Session::Session(std::unique_ptr<SessionImpl> session)
    : _id(sessionIdCounter.addAndFetch(1)), _session(std::move(session)) {}

Session::~Session() = default;

Session::Session(Session&& other) = default;
Session& Session::operator=(Session&& other) = default;

const HostAndPort& Session::remote() const {
    return _session->remote();
}

const HostAndPort& Session::local() const {
    return _session->local();
}

void Session::replaceTags(TagMask tags) {
    _session->replaceTags(tags);
    _session->getTransportLayer()->registerTags(shared_from_this());
}

Session::TagMask Session::getTags() const {
    return _session->getTags();
}

Ticket Session::sourceMessage(Message* message, Date_t expiration) {
    return _session->getTransportLayer()->sourceMessage(shared_from_this(), message, expiration);
}

Ticket Session::sinkMessage(const Message& message, Date_t expiration) {
    return _session->getTransportLayer()->sinkMessage(shared_from_this(), message, expiration);
}

SSLPeerInfo Session::getX509PeerInfo() const {
    return _session->getX509PeerInfo();
}

TransportLayer* Session::getTransportLayer() const {
    return _session->getTransportLayer();
}

MessageCompressorManager& Session::getCompressorManager() {
    return _session->getCompressorManager();
}

}  // namespace transport
}  // namespace mongo
