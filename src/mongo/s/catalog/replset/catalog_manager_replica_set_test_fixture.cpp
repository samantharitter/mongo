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

#include "mongo/platform/basic.h"

#include "mongo/s/catalog/replset/catalog_manager_replica_set_test_fixture.h"

#include <vector>

#include "mongo/base/status_with.h"
#include "mongo/client/remote_command_runner_mock.h"
#include "mongo/client/remote_command_targeter_factory_mock.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/cursor_responses.h"
#include "mongo/db/repl/replication_executor.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/dist_lock_manager_mock.h"
#include "mongo/s/catalog/replset/catalog_manager_replica_set.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/grid.h"
#include "mongo/stdx/memory.h"

namespace mongo {

    using executor::NetworkInterfaceMock;
    using std::vector;

    CatalogManagerReplSetTestFixture::CatalogManagerReplSetTestFixture() = default;

    CatalogManagerReplSetTestFixture::~CatalogManagerReplSetTestFixture() = default;

    void CatalogManagerReplSetTestFixture::setUp() {
        std::unique_ptr<NetworkInterfaceMock> network(
            stdx::make_unique<executor::NetworkInterfaceMock>());

        _mockNetwork = network.get();

        std::unique_ptr<repl::ReplicationExecutor> executor(
            stdx::make_unique<repl::ReplicationExecutor>(network.release(),
                                                         nullptr,
                                                         0));

        // The executor thread might run after the executor unique_ptr above has been moved to the
        // ShardRegistry, so make sure we get the underlying pointer before that.
        _executorThread = std::thread(std::bind([](repl::ReplicationExecutor* executorPtr) {
                                                    executorPtr->run();
                                                },
                                                executor.get()));

        std::unique_ptr<CatalogManagerReplicaSet> cm(
            stdx::make_unique<CatalogManagerReplicaSet>());

        ASSERT_OK(cm->init(ConnectionString::forReplicaSet("CatalogManagerReplSetTest",
                                                           { HostAndPort{ "TestHost1" },
                                                             HostAndPort{ "TestHost2" } }),
                           stdx::make_unique<DistLockManagerMock>()));

        std::unique_ptr<ShardRegistry> shardRegistry(
            stdx::make_unique<ShardRegistry>(stdx::make_unique<RemoteCommandTargeterFactoryMock>(),
                                             stdx::make_unique<RemoteCommandRunnerMock>(),
                                             std::move(executor),
                                             cm.get()));

        // For now initialize the global grid object. All sharding objects will be accessible
        // from there until we get rid of it.
        grid.init(std::move(cm), std::move(shardRegistry));
    }

    void CatalogManagerReplSetTestFixture::tearDown() {
        // Stop the executor and wait for the executor thread to complete. This means that there
        // will be no more calls into the executor and it can be safely deleted.
        shardRegistry()->getExecutor()->shutdown();
        _executorThread.join();

        // This call will delete the shard registry, which will terminate the executor
        grid.clearForUnitTests();
    }

    CatalogManagerReplicaSet* CatalogManagerReplSetTestFixture::catalogManager() const {
        auto cm = dynamic_cast<CatalogManagerReplicaSet*>(grid.catalogManager());
        invariant(cm);

        return cm;
    }

    ShardRegistry* CatalogManagerReplSetTestFixture::shardRegistry() const {
        return grid.shardRegistry();
    }

    RemoteCommandRunnerMock* CatalogManagerReplSetTestFixture::commandRunner() const {
        return RemoteCommandRunnerMock::get(shardRegistry()->getCommandRunner());
    }

    executor::NetworkInterfaceMock* CatalogManagerReplSetTestFixture::network() const {
        return _mockNetwork;
    }

    DistLockManagerMock* CatalogManagerReplSetTestFixture::distLock() const {
        auto distLock = dynamic_cast<DistLockManagerMock*>(catalogManager()->getDistLockManager());
        invariant(distLock);

        return distLock;
    }

    void CatalogManagerReplSetTestFixture::onCommand(OnCommandFunction func) {
        network()->enterNetwork();

        const NetworkInterfaceMock::NetworkOperationIterator noi =
            network()->getNextReadyRequest();
        const RemoteCommandRequest& request = noi->getRequest();

        const auto& resultStatus = func(request);

        BSONObjBuilder result;

        if (resultStatus.isOK()) {
            result.appendElements(resultStatus.getValue());
        }

        Command::appendCommandStatus(result, resultStatus.getStatus());

        const RemoteCommandResponse response(result.obj(), Milliseconds(1));

        network()->scheduleResponse(noi, network()->now(), response);

        network()->runReadyNetworkOperations();

        network()->exitNetwork();
    }

    void CatalogManagerReplSetTestFixture::onFindCommand(OnFindCommandFunction func) {
        onCommand([&func](const RemoteCommandRequest& request) -> StatusWith<BSONObj> {

            const auto& resultStatus = func(request);

            if (!resultStatus.isOK()) {
                return resultStatus.getStatus();
            }

            BSONArrayBuilder arr;
            for (const auto& obj : resultStatus.getValue()) {
                arr.append(obj);
            }

            const NamespaceString nss = NamespaceString(request.dbname,
                                                        request.cmdObj.firstElement().String());
            BSONObjBuilder result;
            appendCursorResponseObject(0LL, nss.toString(), arr.arr(), &result);

            return result.obj();
        });
    }

} // namespace mongo
