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

#define MONGO_LOG_DEFAULT_COMPONENT ::mongo::logger::LogComponent::kSharding

#include "mongo/platform/basic.h"

#include <chrono>
#include <future>

#include "mongo/client/dbclientinterface.h"
#include "mongo/client/remote_command_targeter_mock.h"
#include "mongo/db/commands.h"
#include "mongo/db/query/lite_parsed_query.h"
#include "mongo/executor/network_interface_mock.h"
#include "mongo/executor/task_executor.h"
#include "mongo/s/catalog/dist_lock_manager_mock.h"
#include "mongo/s/catalog/replset/catalog_manager_replica_set.h"
#include "mongo/s/catalog/replset/catalog_manager_replica_set_test_fixture.h"
#include "mongo/s/catalog/type_chunk.h"
#include "mongo/s/catalog/type_collection.h"
#include "mongo/s/catalog/type_database.h"
#include "mongo/s/catalog/type_shard.h"
#include "mongo/s/chunk_version.h"
#include "mongo/s/catalog/type_settings.h"
#include "mongo/s/client/shard_registry.h"
#include "mongo/s/write_ops/batched_command_response.h"
#include "mongo/s/write_ops/batched_update_request.h"
#include "mongo/util/log.h"

namespace mongo {
namespace {

    using executor::NetworkInterfaceMock;
    using executor::TaskExecutor;
    using std::async;
    using std::string;
    using std::vector;
    using stdx::chrono::milliseconds;
    using unittest::assertGet;

    static const std::chrono::seconds kFutureTimeout{5};

    TEST_F(CatalogManagerReplSetTestFixture, GetCollectionExisting) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        CollectionType expectedColl;
        expectedColl.setNs(NamespaceString("TestDB.TestNS"));
        expectedColl.setKeyPattern(BSON("KeyName" << 1));
        expectedColl.setUpdatedAt(Date_t());
        expectedColl.setEpoch(OID::gen());

        auto future = async(std::launch::async, [this, &expectedColl] {
            return assertGet(catalogManager()->getCollection(expectedColl.getNs()));
        });

        onFindCommand([&expectedColl](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), CollectionType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            // Ensure the query is correct
            ASSERT_EQ(query->ns(), CollectionType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(CollectionType::fullNs(expectedColl.getNs())));

            return vector<BSONObj>{ expectedColl.toBSON() };
        });

        // Now wait for the getCollection call to return
        const auto& actualColl = future.get();
        ASSERT_EQ(expectedColl.toBSON(), actualColl.toBSON());
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetCollectionNotExisting) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            auto status = catalogManager()->getCollection("NonExistent");
            ASSERT_EQUALS(status.getStatus(), ErrorCodes::NamespaceNotFound);
        });

        onFindCommand([](const RemoteCommandRequest& request) {
            return vector<BSONObj>{ };
        });

        // Now wait for the getCollection call to return
        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetDatabaseExisting) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        DatabaseType expectedDb;
        expectedDb.setName("bigdata");
        expectedDb.setPrimary("shard0000");
        expectedDb.setSharded(true);

        auto future = async(std::launch::async, [this, &expectedDb] {
            return assertGet(catalogManager()->getDatabase(expectedDb.getName()));
        });

        onFindCommand([&expectedDb](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), DatabaseType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), DatabaseType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(DatabaseType::name(expectedDb.getName())));

            return vector<BSONObj>{ expectedDb.toBSON() };
        });

        const auto& actualDb = future.get();
        ASSERT_EQ(expectedDb.toBSON(), actualDb.toBSON());
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetDatabaseNotExisting) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            auto dbResult = catalogManager()->getDatabase("NonExistent");
            ASSERT_EQ(dbResult.getStatus(), ErrorCodes::NamespaceNotFound);
        });

        onFindCommand([](const RemoteCommandRequest& request) {
            return vector<BSONObj>{ };
        });

        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, UpdateCollection) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        CollectionType collection;
        collection.setNs(NamespaceString("db.coll"));
        collection.setUpdatedAt(network()->now());
        collection.setUnique(true);
        collection.setEpoch(OID::gen());
        collection.setKeyPattern(KeyPattern(BSON("_id" << 1)));

        auto future = async(std::launch::async, [this, collection] {
            auto status = catalogManager()->updateCollection(collection.getNs().toString(),
                                                             collection);
            ASSERT_OK(status);
        });

        onCommand([collection](const RemoteCommandRequest& request) {
            ASSERT_EQUALS("config", request.dbname);

            BatchedUpdateRequest actualBatchedUpdate;
            std::string errmsg;
            ASSERT_TRUE(actualBatchedUpdate.parseBSON(request.cmdObj, &errmsg));
            ASSERT_EQUALS(CollectionType::ConfigNS, actualBatchedUpdate.getCollName());
            auto updates = actualBatchedUpdate.getUpdates();
            ASSERT_EQUALS(1U, updates.size());
            auto update = updates.front();

            ASSERT_TRUE(update->getUpsert());
            ASSERT_FALSE(update->getMulti());
            ASSERT_EQUALS(update->getQuery(),
                          BSON(CollectionType::fullNs(collection.getNs().toString())));
            ASSERT_EQUALS(update->getUpdateExpr(), collection.toBSON());

            BatchedCommandResponse response;
            response.setOk(true);
            response.setNModified(1);

            return response.toBSON();
        });

        // Now wait for the updateCollection call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, UpdateCollectionNotMaster) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        CollectionType collection;
        collection.setNs(NamespaceString("db.coll"));
        collection.setUpdatedAt(network()->now());
        collection.setUnique(true);
        collection.setEpoch(OID::gen());
        collection.setKeyPattern(KeyPattern(BSON("_id" << 1)));

        auto future = async(std::launch::async, [this, collection] {
            auto status = catalogManager()->updateCollection(collection.getNs().toString(),
                                                             collection);
            ASSERT_EQUALS(ErrorCodes::NotMaster, status);
        });

        for (int i = 0; i < 3; ++i) {
            onCommand([](const RemoteCommandRequest& request) {
                BatchedCommandResponse response;
                response.setOk(false);
                response.setErrCode(ErrorCodes::NotMaster);
                response.setErrMessage("not master");

                return response.toBSON();
            });
        }

        // Now wait for the updateCollection call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, UpdateCollectionNotMasterRetrySuccess) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        HostAndPort host1("TestHost1");
        HostAndPort host2("TestHost2");
        targeter->setFindHostReturnValue(host1);

        CollectionType collection;
        collection.setNs(NamespaceString("db.coll"));
        collection.setUpdatedAt(network()->now());
        collection.setUnique(true);
        collection.setEpoch(OID::gen());
        collection.setKeyPattern(KeyPattern(BSON("_id" << 1)));

        auto future = async(std::launch::async, [this, collection] {
            auto status = catalogManager()->updateCollection(collection.getNs().toString(),
                                                             collection);
            ASSERT_OK(status);
        });

        onCommand([host1, host2, targeter](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(host1, request.target);

            BatchedCommandResponse response;
            response.setOk(false);
            response.setErrCode(ErrorCodes::NotMaster);
            response.setErrMessage("not master");

            // Ensure that when the catalog manager tries to retarget after getting the
            // NotMaster response, it will get back a new target.
            targeter->setFindHostReturnValue(host2);
            return response.toBSON();
        });

        onCommand([host2, collection](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(host2, request.target);

            BatchedUpdateRequest actualBatchedUpdate;
            std::string errmsg;
            ASSERT_TRUE(actualBatchedUpdate.parseBSON(request.cmdObj, &errmsg));
            ASSERT_EQUALS(CollectionType::ConfigNS, actualBatchedUpdate.getCollName());
            auto updates = actualBatchedUpdate.getUpdates();
            ASSERT_EQUALS(1U, updates.size());
            auto update = updates.front();

            ASSERT_TRUE(update->getUpsert());
            ASSERT_FALSE(update->getMulti());
            ASSERT_EQUALS(update->getQuery(),
                          BSON(CollectionType::fullNs(collection.getNs().toString())));
            ASSERT_EQUALS(update->getUpdateExpr(), collection.toBSON());

            BatchedCommandResponse response;
            response.setOk(true);
            response.setNModified(1);

            return response.toBSON();
        });

        // Now wait for the updateCollection call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetAllShardsValid) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        ShardType s1;
        s1.setName("shard0000");
        s1.setHost("ShardHost");
        s1.setDraining(false);
        s1.setMaxSizeMB(50);
        s1.setTags({ "tag1", "tag2", "tag3" });

        ShardType s2;
        s2.setName("shard0001");
        s2.setHost("ShardHost");

        ShardType s3;
        s3.setName("shard0002");
        s3.setHost("ShardHost");
        s3.setMaxSizeMB(65);

        const vector<ShardType> expectedShardsList = { s1, s2, s3 };

        auto future = async(std::launch::async, [this] {
            vector<ShardType> shards;
            ASSERT_OK(catalogManager()->getAllShards(&shards));
            return shards;
        });

        onFindCommand([&s1, &s2, &s3](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), ShardType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), ShardType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSONObj());

            return vector<BSONObj>{ s1.toBSON(), s2.toBSON(), s3.toBSON() };
        });

        const vector<ShardType> actualShardsList = future.get();
        ASSERT_EQ(actualShardsList.size(), expectedShardsList.size());

        for (size_t i = 0; i < actualShardsList.size(); ++i) {
            ASSERT_EQ(actualShardsList[i].toBSON(), expectedShardsList[i].toBSON());
        }
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetAllShardsWithInvalidShard) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            vector<ShardType> shards;
            Status status = catalogManager()->getAllShards(&shards);

            ASSERT_NOT_OK(status);
            ASSERT_EQ(0U, shards.size());
        });

        onFindCommand([](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), ShardType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), ShardType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSONObj());

            // valid ShardType
            ShardType s1;
            s1.setName("shard0001");
            s1.setHost("ShardHost");

            return vector<BSONObj> {
                s1.toBSON(),
                BSONObj() // empty document is invalid
            };
        });

        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetChunksForNS) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        OID oid = OID::gen();

        ChunkType chunkA;
        chunkA.setName("chunk0000");
        chunkA.setNS("TestDB.TestColl");
        chunkA.setMin(BSON("a" << 1));
        chunkA.setMax(BSON("a" << 100));
        chunkA.setVersion({ 1, 2, oid });
        chunkA.setShard("shard0000");

        ChunkType chunkB;
        chunkB.setName("chunk0001");
        chunkB.setNS("TestDB.TestColl");
        chunkB.setMin(BSON("a" << 100));
        chunkB.setMax(BSON("a" << 200));
        chunkB.setVersion({ 3, 4, oid });
        chunkB.setShard("shard0001");

        ChunkVersion queryChunkVersion({ 1, 2, oid });

        const Query chunksQuery(BSON(ChunkType::ns("TestDB.TestColl") <<
                                     ChunkType::DEPRECATED_lastmod() <<
                                        BSON("$gte" << static_cast<long long>(
                                                            queryChunkVersion.toLong()))));

        auto future = async(std::launch::async, [this, &chunksQuery] {
            vector<ChunkType> chunks;

            ASSERT_OK(catalogManager()->getChunks(chunksQuery, 0, &chunks));
            ASSERT_EQ(2U, chunks.size());

            return chunks;
        });

        onFindCommand([&chunksQuery, chunkA, chunkB](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), ChunkType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), ChunkType::ConfigNS);
            ASSERT_EQ(query->getFilter(), chunksQuery.getFilter());

            return vector<BSONObj>{ chunkA.toBSON(), chunkB.toBSON() };
        });

        const auto& chunks = future.get();
        ASSERT_EQ(chunkA.toBSON(), chunks[0].toBSON());
        ASSERT_EQ(chunkB.toBSON(), chunks[1].toBSON());
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetChunksForNSNoChunks) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        ChunkVersion queryChunkVersion({ 1, 2, OID::gen() });

        const Query chunksQuery(BSON(ChunkType::ns("TestDB.TestColl") <<
                                     ChunkType::DEPRECATED_lastmod() <<
                                        BSON("$gte" << static_cast<long long>(
                                                            queryChunkVersion.toLong()))));

        auto future = async(std::launch::async, [this, &chunksQuery] {
            vector<ChunkType> chunks;

            ASSERT_OK(catalogManager()->getChunks(chunksQuery, 0, &chunks));
            ASSERT_EQ(0U, chunks.size());
        });

        onFindCommand([&chunksQuery](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), ChunkType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), ChunkType::ConfigNS);
            ASSERT_EQ(query->getFilter(), chunksQuery.getFilter());

            return vector<BSONObj>{ };
        });

        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetChunksForNSInvalidChunk) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        ChunkVersion queryChunkVersion({ 1, 2, OID::gen() });

        const Query chunksQuery(BSON(ChunkType::ns("TestDB.TestColl") <<
                                     ChunkType::DEPRECATED_lastmod() <<
                                        BSON("$gte" << static_cast<long long>(
                                                            queryChunkVersion.toLong()))));

        auto future = async(std::launch::async, [this, &chunksQuery] {
            vector<ChunkType> chunks;
            Status status = catalogManager()->getChunks(chunksQuery, 0, &chunks);

            ASSERT_EQUALS(ErrorCodes::FailedToParse, status);
            ASSERT_EQ(0U, chunks.size());
        });

        onFindCommand([&chunksQuery](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), ChunkType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), ChunkType::ConfigNS);
            ASSERT_EQ(query->getFilter(), chunksQuery.getFilter());

            ChunkType chunkA;
            chunkA.setName("chunk0000");
            chunkA.setNS("TestDB.TestColl");
            chunkA.setMin(BSON("a" << 1));
            chunkA.setMax(BSON("a" << 100));
            chunkA.setVersion({ 1, 2, OID::gen() });
            chunkA.setShard("shard0000");

            ChunkType chunkB;
            chunkB.setName("chunk0001");
            chunkB.setNS("TestDB.TestColl");
            chunkB.setMin(BSON("a" << 100));
            chunkB.setMax(BSON("a" << 200));
            chunkB.setVersion({ 3, 4, OID::gen() });
            // Missing shard id

            return vector<BSONObj>{ chunkA.toBSON(), chunkB.toBSON() };
        });

        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementReadCommand) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            BSONObjBuilder responseBuilder;
            bool ok = catalogManager()->runUserManagementReadCommand("test",
                                                                     BSON("usersInfo" << 1),
                                                                     &responseBuilder);
            ASSERT_TRUE(ok);

            BSONObj response = responseBuilder.obj();
            ASSERT_TRUE(response["ok"].trueValue());
            auto users = response["users"].Array();
            ASSERT_EQUALS(0U, users.size());
        });

        onCommand([](const RemoteCommandRequest& request) {
            ASSERT_EQUALS("test", request.dbname);
            ASSERT_EQUALS(BSON("usersInfo" << 1), request.cmdObj);

            return BSON("ok" << 1 << "users" << BSONArrayBuilder().arr());
        });

        // Now wait for the runUserManagementReadCommand call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementReadCommandUnsatisfiedReadPref) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(Status(ErrorCodes::FailedToSatisfyReadPreference,
                                                "no nodes up"));

        BSONObjBuilder responseBuilder;
        bool ok = catalogManager()->runUserManagementReadCommand("test",
                                                                 BSON("usersInfo" << 1),
                                                                 &responseBuilder);
        ASSERT_FALSE(ok);

        Status commandStatus = Command::getStatusFromCommandResult(responseBuilder.obj());
        ASSERT_EQUALS(ErrorCodes::FailedToSatisfyReadPreference, commandStatus);
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementWriteCommandDistLockHeld) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        distLock()->expectLock(
                [](StringData name,
                   StringData whyMessage,
                   milliseconds waitFor,
                   milliseconds lockTryInterval) {
            ASSERT_EQUALS("authorizationData", name);
            ASSERT_EQUALS("dropUser", whyMessage);
        }, Status(ErrorCodes::LockBusy, "lock already held"));

        BSONObjBuilder responseBuilder;
        bool ok = catalogManager()->runUserManagementWriteCommand("dropUser",
                                                                  "test",
                                                                  BSON("dropUser" << "test"),
                                                                  &responseBuilder);
        ASSERT_FALSE(ok);
        BSONObj response = responseBuilder.obj();
        ASSERT_EQUALS(ErrorCodes::LockBusy, Command::getStatusFromCommandResult(response));
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementWriteCommandSuccess) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        distLock()->expectLock(
                [](StringData name,
                   StringData whyMessage,
                   milliseconds waitFor,
                   milliseconds lockTryInterval) {
            ASSERT_EQUALS("authorizationData", name);
            ASSERT_EQUALS("dropUser", whyMessage);
        }, Status::OK());

        auto future = async(std::launch::async, [this] {
            BSONObjBuilder responseBuilder;
            bool ok = catalogManager()->runUserManagementWriteCommand("dropUser",
                                                                      "test",
                                                                      BSON("dropUser" << "test"),
                                                                      &responseBuilder);
            ASSERT_FALSE(ok);

            Status commandStatus = Command::getStatusFromCommandResult(responseBuilder.obj());
            ASSERT_EQUALS(ErrorCodes::UserNotFound, commandStatus);
        });

        onCommand([](const RemoteCommandRequest& request) {
            ASSERT_EQUALS("test", request.dbname);
            ASSERT_EQUALS(BSON("dropUser" << "test"), request.cmdObj);

            BSONObjBuilder responseBuilder;
            Command::appendCommandStatus(responseBuilder,
                                         Status(ErrorCodes::UserNotFound,
                                                "User test@test not found"));
            return responseBuilder.obj();
        });

        // Now wait for the runUserManagementWriteCommand call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementWriteCommandNotMaster) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        distLock()->expectLock(
                [](StringData name,
                   StringData whyMessage,
                   milliseconds waitFor,
                   milliseconds lockTryInterval) {
            ASSERT_EQUALS("authorizationData", name);
            ASSERT_EQUALS("dropUser", whyMessage);
        }, Status::OK());

        auto future = async(std::launch::async, [this] {
            BSONObjBuilder responseBuilder;
            bool ok = catalogManager()->runUserManagementWriteCommand("dropUser",
                                                                      "test",
                                                                      BSON("dropUser" << "test"),
                                                                      &responseBuilder);
            ASSERT_FALSE(ok);

            Status commandStatus = Command::getStatusFromCommandResult(responseBuilder.obj());
            ASSERT_EQUALS(ErrorCodes::NotMaster, commandStatus);
        });

        for (int i = 0; i < 3; ++i) {
            onCommand([](const RemoteCommandRequest& request) {
                BSONObjBuilder responseBuilder;
                Command::appendCommandStatus(responseBuilder,
                                             Status(ErrorCodes::NotMaster, "not master"));
                return responseBuilder.obj();
            });
        }

        // Now wait for the runUserManagementWriteCommand call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, RunUserManagementWriteCommandNotMasterRetrySuccess) {
        HostAndPort host1("TestHost1");
        HostAndPort host2("TestHost2");
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(host1);

        distLock()->expectLock(
                [](StringData name,
                   StringData whyMessage,
                   milliseconds waitFor,
                   milliseconds lockTryInterval) {
            ASSERT_EQUALS("authorizationData", name);
            ASSERT_EQUALS("dropUser", whyMessage);
        }, Status::OK());

        auto future = async(std::launch::async, [this] {
            BSONObjBuilder responseBuilder;
            bool ok = catalogManager()->runUserManagementWriteCommand("dropUser",
                                                                      "test",
                                                                      BSON("dropUser" << "test"),
                                                                      &responseBuilder);
            ASSERT_TRUE(ok);

            Status commandStatus = Command::getStatusFromCommandResult(responseBuilder.obj());
            ASSERT_OK(commandStatus);
        });

        onCommand([targeter, host1, host2](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(host1, request.target);

            BSONObjBuilder responseBuilder;
            Command::appendCommandStatus(responseBuilder,
                                         Status(ErrorCodes::NotMaster, "not master"));

            // Ensure that when the catalog manager tries to retarget after getting the
            // NotMaster response, it will get back a new target.
            targeter->setFindHostReturnValue(host2);
            return responseBuilder.obj();
        });

        onCommand([host2](const RemoteCommandRequest& request) {
            ASSERT_EQUALS(host2, request.target);
            ASSERT_EQUALS("test", request.dbname);
            ASSERT_EQUALS(BSON("dropUser" << "test"), request.cmdObj);

            return BSON("ok" << 1);
        });

        // Now wait for the runUserManagementWriteCommand call to return
        future.wait_for(kFutureTimeout);
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetGlobalSettingsBalancerDoc) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        // sample balancer doc
        SettingsType st1;
        st1.setKey(SettingsType::BalancerDocKey);
        st1.setBalancerStopped(true);

        auto future = async(std::launch::async, [this] {
            return assertGet(catalogManager()->getGlobalSettings(SettingsType::BalancerDocKey));
        });

        onFindCommand([st1](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), SettingsType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), SettingsType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(SettingsType::key(SettingsType::BalancerDocKey)));

            return vector<BSONObj>{ st1.toBSON() };
        });

        const auto& actualBalSettings = future.get();
        ASSERT_EQ(actualBalSettings.toBSON(), st1.toBSON());
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetGlobalSettingsChunkSizeDoc) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        // sample chunk size doc
        SettingsType st1;
        st1.setKey(SettingsType::ChunkSizeDocKey);
        st1.setChunkSizeMB(80);

        auto future = async(std::launch::async, [this] {
            return assertGet(catalogManager()->getGlobalSettings(SettingsType::ChunkSizeDocKey));
        });

        onFindCommand([st1](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), SettingsType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), SettingsType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(SettingsType::key(SettingsType::ChunkSizeDocKey)));

            return vector<BSONObj>{ st1.toBSON() };
        });

        const auto& actualBalSettings = future.get();
        ASSERT_EQ(actualBalSettings.toBSON(), st1.toBSON());
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetGlobalSettingsInvalidDoc) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            const auto balSettings = catalogManager()->getGlobalSettings("invalidKey");

            ASSERT_EQ(balSettings.getStatus(), ErrorCodes::FailedToParse);
        });

        onFindCommand([](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), SettingsType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), SettingsType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(SettingsType::key("invalidKey")));

            return vector<BSONObj> {
                BSON("invalidKey" << "some value") // invalid settings document -- key is required
            };
        });

        future.get();
    }

    TEST_F(CatalogManagerReplSetTestFixture, GetGlobalSettingsNonExistent) {
        RemoteCommandTargeterMock* targeter =
            RemoteCommandTargeterMock::get(shardRegistry()->getShard("config")->getTargeter());
        targeter->setFindHostReturnValue(HostAndPort("TestHost1"));

        auto future = async(std::launch::async, [this] {
            const auto chunkSizeSettings = catalogManager()->getGlobalSettings(
                SettingsType::ChunkSizeDocKey);

            ASSERT_EQ(chunkSizeSettings.getStatus(), ErrorCodes::NoMatchingDocument);
        });

        onFindCommand([](const RemoteCommandRequest& request) {
            const NamespaceString nss(request.dbname, request.cmdObj.firstElement().String());
            ASSERT_EQ(nss.toString(), SettingsType::ConfigNS);

            auto query = assertGet(
                LiteParsedQuery::makeFromFindCommand(nss, request.cmdObj, false));

            ASSERT_EQ(query->ns(), SettingsType::ConfigNS);
            ASSERT_EQ(query->getFilter(), BSON(SettingsType::key(SettingsType::ChunkSizeDocKey)));

            return vector<BSONObj> { };
        });

        future.get();
    }

} // namespace
} // namespace mongo
