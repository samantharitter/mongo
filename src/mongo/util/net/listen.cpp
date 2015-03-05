// listen.cpp

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

#include "mongo/util/net/listen.h"

#include <boost/scoped_array.hpp>
#include <boost/shared_ptr.hpp>

#include "mongo/db/server_options.h"
#include "mongo/base/owned_pointer_vector.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/scopeguard.h"

# ifndef __sunos__
#  include <ifaddrs.h>
# endif
# include <sys/resource.h>
# include <sys/stat.h>

#include <sys/types.h>
#include <sys/socket.h>
#include <sys/un.h>
#include <netinet/in.h>
#include <netinet/tcp.h>
#include <arpa/inet.h>
#include <errno.h>
#include <netdb.h>
#ifdef __openbsd__
# include <sys/uio.h>
#endif

namespace mongo {

    using boost::shared_ptr;
    using std::endl;
    using std::string;
    using std::vector;

    // ----- Listener -------

    const Listener* Listener::_timeTracker;

    vector<SockAddr> ipToAddrs(const char* ips, int port, bool useUnixSockets) {
        vector<SockAddr> out;
        if (*ips == '\0') {
            out.push_back(SockAddr("0.0.0.0", port)); // IPv4 all

            if (IPv6Enabled())
                out.push_back(SockAddr("::", port)); // IPv6 all
            if (useUnixSockets)
                out.push_back(SockAddr(makeUnixSockPath(port).c_str(), port)); // Unix socket
            return out;
        }

        while(*ips) {
            string ip;
            const char * comma = strchr(ips, ',');
            if (comma) {
                ip = string(ips, comma - ips);
                ips = comma + 1;
            }
            else {
                ip = string(ips);
                ips = "";
            }

            SockAddr sa(ip.c_str(), port);
            out.push_back(sa);

            if (sa.isValid() && useUnixSockets &&
                    (sa.getAddr() == "127.0.0.1" || sa.getAddr() == "0.0.0.0")) // only IPv4
                out.push_back(SockAddr(makeUnixSockPath(port).c_str(), port));
        }
        return out;

    }

    Listener::Listener(const string& name, const string &ip, int port, bool logConnect )
        : _port(port), _name(name), _ip(ip), _setupSocketsSuccessful(false),
          _logConnect(logConnect), _elapsedTime(0) {
    }

    Listener::~Listener() {
        if ( _timeTracker == this )
            _timeTracker = 0;
    }

    void Listener::setupSockets() {

        // set up our single socket with the right address and stuff
        _addr = ipToAddrs(_ip.c_str(), _port, (!serverGlobalParams.noUnixSocket &&
                                               useUnixSockets())).front();

        const SockAddr& me = _addr;

        if (!me.isValid()) {
            error() << "listen(): SockAddr is invalid." << endl;
            return;
        }

        SOCKET sock = ::socket(AF_INET, SOCK_DGRAM, 0);
        ScopeGuard socketGuard = MakeGuard(&closesocket, sock);
        massert( 15863 , str::stream() << "listen(): invalid socket? " << errnoWithDescription() , sock >= 0 );

        {
            const int one = 1;
            if ( setsockopt( sock , SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) < 0 )
                log() << "Failed to set socket opt, SO_REUSEADDR" << endl;
        }

        if ( ::bind(sock, me.raw(), me.addressSize) != 0 ) {
            int x = errno;
            error() << "listen(): bind() failed " << errnoWithDescription(x) << " for socket: " << me.toString() << endl;
            if ( x == EADDRINUSE )
                error() << "  addr already in use" << endl;
            return;
        }

        std::cout << "bound socket!" << std::endl;

        _sock = sock;
        socketGuard.Dismiss();
        _setupSocketsSuccessful = true;
    }

    void Listener::initAndListen() {
        struct sockaddr_in remote;
        socklen_t addrlen = sizeof(remote);
        unsigned char buf [100];
        int recvlen;

        std::cout << "entering initAndListen..." << std::endl;

        // what we really want to do here is just open one socket and listen on it

        if (!_setupSocketsSuccessful) {
            std::cout << "didn't set up sockets successfully, exiting" << std::endl;
            return;
        }

        _logListen(_port, false);

        {
            // Wake up any threads blocked in waitUntilListening()
            boost::lock_guard<boost::mutex> lock(_readyMutex);
            _ready = true;
            _readyCondition.notify_all();
        }

        std::cout << "entering listening loop..." << std::endl;
        while ( ! inShutdown() ) {
            recvlen = recvfrom(_sock, buf, 100, 0, (struct sockaddr *)&remote, &addrlen);
            if (recvlen > 0) {
                buf[recvlen] = 0;
                std::cout << "SERVER: " << buf << std::endl;
            }
        }
    }

    void Listener::_logListen( int port , bool ssl ) {
        log() << _name << ( _name.size() ? " " : "" ) << "waiting for packets on port " << port << ( ssl ? " ssl" : "" ) << endl;
    }

    void Listener::waitUntilListening() const {
        boost::unique_lock<boost::mutex> lock(_readyMutex);
        while (!_ready) {
            _readyCondition.wait(lock);
        }
    }

    void Listener::accepted(boost::shared_ptr<Socket> psocket, long long connectionId ) {
        MessagingPort* port = new MessagingPort(psocket);
        port->setConnectionId( connectionId );
        acceptedMP( port );
    }

    void Listener::acceptedMP(MessagingPort *mp) {
        verify(!"You must overwrite one of the accepted methods");
    }

    // ----- ListeningSockets -------

    ListeningSockets* ListeningSockets::_instance = new ListeningSockets();

    ListeningSockets* ListeningSockets::get() {
        return _instance;
    }

    // ------ connection ticket and control ------

    TicketHolder Listener::globalTicketHolder(DEFAULT_MAX_CONN);
    AtomicInt64 Listener::globalConnectionNumber;

    void ListeningSockets::closeAll() {
        std::set<int>* sockets;
        std::set<std::string>* paths;

        {
            scoped_lock lk( _mutex );
            sockets = _sockets;
            _sockets = new std::set<int>();
            paths = _socketPaths;
            _socketPaths = new std::set<std::string>();
        }

        for ( std::set<int>::iterator i=sockets->begin(); i!=sockets->end(); i++ ) {
            int sock = *i;
            log() << "closing listening socket: " << sock << std::endl;
            closesocket( sock );
        }
        delete sockets;

        for ( std::set<std::string>::iterator i=paths->begin(); i!=paths->end(); i++ ) {
            std::string path = *i;
            log() << "removing socket file: " << path << std::endl;
            ::remove( path.c_str() );
        }
        delete paths;
    }

}
