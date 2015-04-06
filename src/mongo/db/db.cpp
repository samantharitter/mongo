// @file db.cpp : Defines main() for the mongod program.

/**
*    Copyright (C) 2008-2014 MongoDB Inc.
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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kStorage

#include "mongo/platform/basic.h"

#include <boost/thread/thread.hpp>
#include <boost/filesystem/operations.hpp>
#include <boost/shared_ptr.hpp>
#include <fstream>
#include <iostream>
#include <limits>
#include <signal.h>
#include <string>
#include <thread>

#include "mongo/base/init.h"
#include "mongo/base/initializer.h"
#include "mongo/base/status.h"
#include "mongo/config.h"
#include "mongo/db/auth/auth_index_d.h"
#include "mongo/db/auth/authorization_manager.h"
#include "mongo/db/auth/authorization_manager_global.h"
#include "mongo/db/auth/authz_manager_external_state_d.h"
#include "mongo/db/catalog/collection.h"
#include "mongo/db/catalog/database.h"
#include "mongo/db/catalog/database_catalog_entry.h"
#include "mongo/db/catalog/index_catalog.h"
#include "mongo/db/catalog/index_key_validate.h"
#include "mongo/db/client.h"
#include "mongo/db/clientcursor.h"
#include "mongo/db/concurrency/d_concurrency.h"
#include "mongo/db/db.h"
#include "mongo/db/db_raii.h"
#include "mongo/db/db_shared.h"
#include "mongo/db/dbdirectclient.h"
#include "mongo/db/dbmessage.h"
#include "mongo/db/dbwebserver.h"
#include "mongo/db/service_context_d.h"
#include "mongo/db/service_context.h"
#include "mongo/db/index_names.h"
#include "mongo/db/index_rebuilder.h"
#include "mongo/db/initialize_server_global_state.h"
#include "mongo/db/instance.h"
#include "mongo/db/introspect.h"
#include "mongo/db/json.h"
#include "mongo/db/lasterror.h"
#include "mongo/db/log_process_details.h"
#include "mongo/db/mongod_options.h"
#include "mongo/db/op_observer.h"
#include "mongo/db/operation_context_impl.h"
#include "mongo/db/query/internal_plans.h"
#include "mongo/db/range_deleter_service.h"
#include "mongo/db/repair_database.h"
#include "mongo/db/repl/network_interface_impl.h"
#include "mongo/db/repl/oplog.h"
#include "mongo/db/repl/repl_settings.h"
#include "mongo/db/repl/replication_coordinator_external_state_impl.h"
#include "mongo/db/repl/replication_coordinator_global.h"
#include "mongo/db/repl/replication_coordinator_impl.h"
#include "mongo/db/repl/topology_coordinator_impl.h"
#include "mongo/db/restapi.h"
#include "mongo/db/server_parameters.h"
#include "mongo/db/startup_warnings_mongod.h"
#include "mongo/db/stats/counters.h"
#include "mongo/db/stats/snapshots.h"
#include "mongo/db/storage/mmap_v1/mmap_v1_options.h"
#include "mongo/db/storage/storage_engine.h"
#include "mongo/db/storage_options.h"
#include "mongo/db/ttl.h"
#include "mongo/platform/process_id.h"
#include "mongo/scripting/engine.h"
#include "mongo/stdx/memory.h"
#include "mongo/util/assert_util.h"
#include "mongo/util/cmdline_utils/censor_cmdline.h"
#include "mongo/util/concurrency/task.h"
#include "mongo/util/concurrency/thread_name.h"
#include "mongo/util/exception_filter_win32.h"
#include "mongo/util/exit.h"
#include "mongo/util/log.h"
#include "mongo/util/net/message_server.h"
#include "mongo/util/net/port_message_server.h"
#include "mongo/util/net/ssl_manager.h"
#include "mongo/util/ntservice.h"
#include "mongo/util/options_parser/startup_options.h"
#include "mongo/util/quick_exit.h"
#include "mongo/util/ramlog.h"
#include "mongo/util/scopeguard.h"
#include "mongo/util/signal_handlers.h"
#include "mongo/util/stacktrace.h"
#include "mongo/util/startup_test.h"
#include "mongo/util/text.h"
#include "mongo/util/time_support.h"
#include "mongo/util/version_reporting.h"

# include <sys/file.h>

#include "asio.hpp"

using namespace mongo;
using std::auto_ptr;
using std::cout;
using std::cerr;
using std::endl;
using std::list;
using std::string;
using std::stringstream;
using std::vector;

using logger::LogComponent;

static int mongoDbMain(int argc, char* argv[], char** envp);

// MAIN.
int main(int argc, char* argv[], char** envp) {
    int exitCode = mongoDbMain(argc, argv, envp);
    quickExit(exitCode);
}

MONGO_INITIALIZER_GENERAL(ForkServer,
                          ("EndStartupOptionHandling"),
                          ("default"))(InitializerContext* context) {
    mongo::forkServerOrDie();
    return Status::OK();
}

/*
 * This function should contain the startup "actions" that we take based on the startup config.  It
 * is intended to separate the actions from "storage" and "validation" of our startup configuration.
 */
static void startupConfigActions(const std::vector<std::string>& args) {
    // The "command" option is deprecated.  For backward compatibility, still support the "run"
    // and "dbppath" command.  The "run" command is the same as just running mongod, so just
    // falls through.
    if (moe::startupOptionsParsed.count("command")) {
        vector<string> command = moe::startupOptionsParsed["command"].as< vector<string> >();

        if (command[0].compare("dbpath") == 0) {
            cout << storageGlobalParams.dbpath << endl;
            quickExit(EXIT_SUCCESS);
        }

        if (command[0].compare("run") != 0) {
            cout << "Invalid command: " << command[0] << endl;
            printMongodHelp(moe::startupOptions);
            quickExit(EXIT_FAILURE);
        }

        if (command.size() > 1) {
            cout << "Too many parameters to 'run' command" << endl;
            printMongodHelp(moe::startupOptions);
            quickExit(EXIT_FAILURE);
        }
    }
}

MONGO_INITIALIZER_GENERAL(CreateAuthorizationManager,
                          ("SetupInternalSecurityUser",
                           "OIDGeneration",
                           "SetGlobalEnvironment",
                           "EndStartupOptionStorage"),
                          MONGO_NO_DEPENDENTS)
        (InitializerContext* context) {
    auto authzManager = stdx::make_unique<AuthorizationManager>(
            new AuthzManagerExternalStateMongod());
    authzManager->setAuthEnabled(serverGlobalParams.isAuthEnabled);
    AuthorizationManager::set(getGlobalServiceContext(), std::move(authzManager));
    return Status::OK();
}

MONGO_INITIALIZER_WITH_PREREQUISITES(CreateReplicationManager, ("SetGlobalEnvironment"))
        (InitializerContext* context) {
    repl::ReplicationCoordinatorImpl* replCoord = new repl::ReplicationCoordinatorImpl(
            getGlobalReplSettings(),
            new repl::ReplicationCoordinatorExternalStateImpl,
            new repl::NetworkInterfaceImpl,
            new repl::TopologyCoordinatorImpl(Seconds(repl::maxSyncSourceLagSecs)),
            static_cast<int64_t>(curTimeMillis64()));
    repl::setGlobalReplicationCoordinator(replCoord);
    repl::setOplogCollectionName();
    getGlobalServiceContext()->registerKillOpListener(replCoord);
    return Status::OK();
}

namespace mongoecho {

    using asio::ip::tcp;

    class session : public std::enable_shared_from_this<session> {
    public:
        session(tcp::socket socket)
            : socket_(std::move(socket)) {
            std::cout << "XXX established echo session ["
                      << socket_.remote_endpoint() << " <-> "
                      << socket_.local_endpoint() << "]"
                      << std::endl;
        }

        ~session() {
            std::cout << "XXX terminating echo session ["
                      << socket_.remote_endpoint() << " <-> "
                      << socket_.local_endpoint() << "]"
                      << std::endl;
        }

        void start() {
            do_read();
        }

    private:
        void do_read() {
            auto self(shared_from_this());
            socket_.async_read_some(
                asio::buffer(data_, max_length),
                [this, self](std::error_code ec, std::size_t length) {
                    if (!ec)
                        do_write(length);
                });
        }

        void do_write(std::size_t length) {
            auto self(shared_from_this());
            asio::async_write(
                socket_, asio::buffer(data_, length),
                [this, self](std::error_code ec, std::size_t /*length*/) {
                    if (!ec)
                        do_read();
                });
        }

        tcp::socket socket_;
        enum { max_length = 1024 };
        char data_[max_length];
    };

    class server {
    public:
        server(asio::io_service& io_service, short port)
            : acceptor_(io_service, tcp::endpoint(tcp::v4(), port)),
              socket_(io_service) {
            do_accept();
        }

    private:
        void do_accept() {
            acceptor_.async_accept(
                socket_,
                [this](std::error_code ec) {
                    if (!ec)
                        std::make_shared<session>(std::move(socket_))->start();
                    do_accept();
                });
        }

        tcp::acceptor acceptor_;
        tcp::socket socket_;
    };

} // namespace

// KEEP THIS HERE
static int mongoDbMain(int argc, char* argv[], char **envp) {
    static StaticObserver staticObserver;

    setupSignalHandlers(false);

    dbExecCommand = argv[0];

    srand(curTimeMicros());

    {
        unsigned x = 0x12345678;
        unsigned char& b = (unsigned char&) x;
        if ( b != 0x78 ) {
            mongo::log(LogComponent::kControl) << "big endian cpus not yet supported" << endl;
            return 33;
        }
    }

    Status status = mongo::runGlobalInitializers(argc, argv, envp);
    if (!status.isOK()) {
        severe(LogComponent::kControl) << "Failed global initialization: " << status;
        quickExit(EXIT_FAILURE);
    }

    startupConfigActions(std::vector<std::string>(argv, argv + argc));
    cmdline_utils::censorArgvArray(argc, argv);

    if (!initializeServerGlobalState())
        quickExit(EXIT_FAILURE);

    // Per SERVER-7434, startSignalProcessingThread() must run after any forks
    // (initializeServerGlobalState()) and before creation of any other threads.
    startSignalProcessingThread();

    StartupTest::runTests();

    std::thread echothread([]() {
            try {
                asio::io_service io_service;
                mongoecho::server s(io_service, 31337);
                io_service.run();
            }
            catch (const std::exception& e) {
                std::cerr << "Echo Server Exception: " << e.what() << "\n";
            }
        });
    echothread.detach();

    ExitCode exitCode = initAndListen(serverGlobalParams.port);
    exitCleanly(exitCode);
    return 0;
}
