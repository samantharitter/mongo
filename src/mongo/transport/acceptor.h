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

namespace mongo {

namespace transport {

class Endpoint;

/**
 * An Acceptor is responsible for generating new Endpoints for the TransportLayer.
 * In a networked transport layer, an implementation of this interface might
 * spin in a single thread, waiting for connections from clients.
 *
 * Once the Acceptor makes an Endpoint, it passes ownership of this object to the
 * TransportLayer.
 */
class Acceptor {
public:
    /**
     * Construct a new Acceptor.
     */
    Acceptor() {}
    virtual ~Acceptor();

    /**
     * Callback that the Acceptor should invoke when it generates new Endpoints.
     */
    using NewEndpointCallback = stdx::function<void(std::unique_ptr<Endpoint>&&)>;

    /**
     * Begin running the Acceptor. This method will not return until the Acceptor
     * is shut down.
     */
    virtual void run(NewEndpointCallback handle) = 0;

    /**
     * Shut down the Acceptor. Run() will return after this method is called
     * and no new Endpoints will be accepted.
     */
    virtual void shutdown() = 0;
};

}  // transport

}  // namespace mongo
