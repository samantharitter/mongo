/**
 *    Copyright (C) 2014 MongoDB Inc.
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

#include "mongo/executor/network_interface.h"


namespace mongo {
namespace executor {

// This is a bitmask with the first bit set. It's used to mark connections that should be kept
// open during stepdowns.
const unsigned int NetworkInterface::kMessagingPortKeepOpen;

NetworkInterface::NetworkInterface() {}
NetworkInterface::~NetworkInterface() {}

const ServiceContext::Decoration<std::unique_ptr<NetworkInterface>>
    NetworkInterface::getNetworkInterface =
        ServiceContext::declareDecoration<std::unique_ptr<NetworkInterface>>();

void NetworkInterface::setGlobalNetworkInterface(
    std::unique_ptr<NetworkInterface> networkInterface) {
    invariant(!NetworkInterface::getNetworkInterface(getGlobalServiceContext()));
    getNetworkInterface(getGlobalServiceContext()) = std::move(networkInterface);
}

NetworkInterface* NetworkInterface::getGlobalNetworkInterfaceOrDie() {
    auto net = NetworkInterface::getNetworkInterface(getGlobalServiceContext()).get();
    invariant(net);
    return net;
}

NetworkInterface* NetworkInterface::getGlobalNetworkInterface() {
    return NetworkInterface::getNetworkInterface(getGlobalServiceContext()).get();
}

}  // namespace executor
}  // namespace mongo
