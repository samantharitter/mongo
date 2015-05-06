/**
 *    Copyright (C) 2015 MongoDB Inc.
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

#include "mongo/util/net/asio_networking_layer.h"

#include <boost/make_shared.hpp>

namespace mongo {

    using asio::ip::tcp;

    AsyncMessageRunner::AsyncMessageRunner() :
        _service() // todo this is going to need a handle to networking layer
    {}

    void AsyncMessageRunner::process(Message& m, int id) {
    }

    AsyncNetworkingLayer::AsyncNetworkingLayer(int port) :
        _service(),
        _acceptor(_service),
        _timer(_service),
        _port(port),
        _connections(0),
        _db() // todo break this out
    {}


    void AsyncNetworkingLayer::startup() {
        // set up our acceptor
        tcp::resolver resolver(_service);
        tcp::endpoint endpoint = *resolver.resolve({"127.0.0.1", std::to_string(_port)});
        _acceptor.open(endpoint.protocol());
        _acceptor.set_option(tcp::acceptor::reuse_address(true));
        _acceptor.bind(endpoint);
        _acceptor.listen();

        do_accept();

        do_stuff();

        _serviceRunner = std::thread([this] {
                _service.run();
                std::cout << "\tASIO: _service.run() ended\n";
            });
        _db.startup();

        std::cout << "\tASIO: listening on port " << _port << "\n";
      }

    void AsyncNetworkingLayer::do_stuff() {
        std::cout << "\tASIO: doing some other stuff...\n" << std::flush;
        // use a timer so we don't spam ourselves
        _timer.expires_from_now(std::chrono::seconds(3));
        _timer.async_wait([this](std::error_code ec) {
                do_stuff();
            });
    }

    void AsyncNetworkingLayer::shutdown() {
        _acceptor.close();
        _service.stop();
        _serviceRunner.join();
        _db.shutdown();
    }

    void AsyncNetworkingLayer::do_accept() {
        SharedSocket sock(boost::make_shared<tcp::socket>(_service));
        _acceptor.async_accept(*sock,
             [this, sock](std::error_code ec) {
                  if (ec) {
                      std::cout << "\tASIO: accept() error\n";
                  } else {
                      std::cout << "\tASIO:: accept() success\n";
                      newConnection(sock);
                  }
                  do_accept();
        });
    }

    void AsyncNetworkingLayer::newConnection(SharedSocket sock) {
        std::cout << "\tASIO: new connection\n";
        int id = _connections++; // todo protect this
        recvMessageHeader(sock, id);
    }

    void AsyncNetworkingLayer::recvMessageHeader(SharedSocket sock, int id) {
        std::cout << "\tASIO: recvMessageHeader\n";
    }

    void AsyncNetworkingLayer::recvMessageBody(SharedSocket sock,
                                               int id,
                                               boost::shared_ptr<MSGHEADER::Value> header,
                                               int headerLen) {
        std::cout << "\tASIO: recvMessageBody\n";
    }

    void AsyncNetworkingLayer::processMessage(SharedSocket sock, int id, Message& m) {
        std::cout << "\tASIO: processMessage\n";
    }

    void AsyncNetworkingLayer::cmdFinished(SharedSocket sock, int id) {
        std::cout << "\tASIO: cmdFinished\n";
    }

} // namespace mongo
