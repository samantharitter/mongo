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

#pragma once

#include <memory>
#include <mutex>
#include <string>
#include <vector>

#include "mongo/bson/bsonobj.h"
#include "mongo/client/connection_string.h"
#include "mongo/s/catalog/catalog_manager.h"

namespace mongo {

    struct HostAndPort;
    class NamespaceString;

    /**
     * Implements the catalog manager for talking to replica set config servers.
     */
    class CatalogManagerReplicaSet final : public CatalogManager {
    public:
        CatalogManagerReplicaSet();
        virtual ~CatalogManagerReplicaSet();

        /**
         * Initializes the catalog manager.
         * Can only be called once for the lifetime of the catalog manager.
         * TODO(spencer): Take pointer to ShardRegistry rather than getting it from the global
         * "grid" object.
         */
        Status init(const ConnectionString& configCS,
                    std::unique_ptr<DistLockManager> distLockManager);

        Status startup(bool upgrade) override;

        ConnectionString connectionString() const override;

        void shutDown() override;

        Status enableSharding(const std::string& dbName) override;

        Status shardCollection(const std::string& ns,
                               const ShardKeyPattern& fieldsAndOrder,
                               bool unique,
                               std::vector<BSONObj>* initPoints,
                               std::set<ShardId>* initShardsIds = nullptr) override;

        StatusWith<std::string> addShard(const std::string& name,
                                         const ConnectionString& shardConnectionString,
                                         const long long maxSize) override;

        StatusWith<ShardDrainingStatus> removeShard(OperationContext* txn,
                                                    const std::string& name) override;

        Status createDatabase(const std::string& dbName) override;

        Status updateDatabase(const std::string& dbName, const DatabaseType& db) override;

        StatusWith<DatabaseType> getDatabase(const std::string& dbName) override;

        Status updateCollection(const std::string& collNs, const CollectionType& coll) override;

        StatusWith<CollectionType> getCollection(const std::string& collNs) override;

        Status getCollections(const std::string* dbName,
                              std::vector<CollectionType>* collections) override;

        Status dropCollection(const std::string& collectionNs) override;

        Status getDatabasesForShard(const std::string& shardName,
                                    std::vector<std::string>* dbs) override;

        Status getChunks(const Query& query,
                         int nToReturn,
                         std::vector<ChunkType>* chunks) override;

        Status getTagsForCollection(const std::string& collectionNs,
                                    std::vector<TagsType>* tags) override;

        StatusWith<std::string> getTagForChunk(const std::string& collectionNs,
                                               const ChunkType& chunk) override;

        Status getAllShards(std::vector<ShardType>* shards) override;

        bool isShardHost(const ConnectionString& shardConnectionString) override;

        bool runUserManagementWriteCommand(const std::string& commandName,
                                           const std::string& dbname,
                                           const BSONObj& cmdObj,
                                           BSONObjBuilder* result) override;

        bool runUserManagementReadCommand(const std::string& dbname,
                                          const BSONObj& cmdObj,
                                          BSONObjBuilder* result) override;

        Status applyChunkOpsDeprecated(const BSONArray& updateOps,
                                       const BSONArray& preCondition) override;

        void logAction(const ActionLogType& actionLog) override;

        void logChange(OperationContext* txn,
                       const std::string& what,
                       const std::string& ns,
                       const BSONObj& detail) override;

        StatusWith<SettingsType> getGlobalSettings(const std::string& key) override;

        void writeConfigServerDirect(const BatchedCommandRequest& request,
                                     BatchedCommandResponse* response) override;

        DistLockManager* getDistLockManager() override;

    private:
        /**
         * Executes 'find' command against the specified host and fetches *all* the results that
         * the host will return until there are no more or until an error is returned.
         *
         * Returns either the complete set of results or an error, never partial results.
         */
        StatusWith<std::vector<BSONObj>> _find(const HostAndPort& host,
                                               const NamespaceString& nss,
                                               const BSONObj& query,
                                               int limit);

        /**
         * Runs a command against the specified host and returns the result.
         */
        StatusWith<BSONObj> _runCommand(const HostAndPort& host,
                                        const std::string& dbName,
                                        const BSONObj& cmdObj);

        // Config server connection string
        ConnectionString _configServerConnectionString;

        // Distribted lock manager singleton.
        std::unique_ptr<DistLockManager> _distLockManager;

        // protects _inShutdown
        std::mutex _mutex;

        // True if shutDown() has been called. False, otherwise.
        bool _inShutdown = false;
    };

} // namespace mongo
