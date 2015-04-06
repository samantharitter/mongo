// message_server_port.cpp

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

#include <boost/scoped_ptr.hpp>
#include <boost/thread/thread.hpp>
#include <memory>

#include "mongo/base/disallow_copying.h"
#include "mongo/config.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/server_options.h"
#include "mongo/db/stats/counters.h"
#include "mongo/stdx/functional.h"
#include "mongo/util/concurrency/synchronization.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/concurrency/ticketholder.h"
#include "mongo/util/debug_util.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/mongoutils/str.h"
#include "mongo/util/net/listen.h"
#include "mongo/util/net/message.h"
#include "mongo/util/net/message_port.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/port_message_server.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/scopeguard.h"

#ifdef __linux__  // TODO: consider making this ifndef _WIN32
# include <sys/resource.h>
#endif

#if !defined(__has_feature)
#define __has_feature(x) 0
#endif

namespace mongo {

    MessageServer * createServer( const MessageServer::Options& opts , MessageHandler * handler ) {
        return new PortMessageServer( opts , handler );
    }

}  // namespace mongo
