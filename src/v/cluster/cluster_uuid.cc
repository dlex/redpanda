/*
 * Copyright 2022 Redpanda Data, Inc.
 *
 * Use of this software is governed by the Business Source License
 * included in the file licenses/BSL.md
 *
 * As of the Change Date specified in that file, in accordance with
 * the Business Source License, use of this software will be governed
 * by the Apache License, Version 2.0
 */

#include "cluster/cluster_uuid.h"

#include "storage/api.h"

namespace cluster {

const bytes cluster_uuid_key = "cluster_uuid";
const storage::kvstore::key_space cluster_uuid_key_space
  = storage::kvstore::key_space::controller;

std::optional<model::cluster_uuid>
read_stored_cluster_uuid(storage::kvstore& kvstore) {
    vassert(
      ss::this_shard_id() == ss::shard_id(0),
      "Cluster UUID is only stored in shard 0");
    std::optional<iobuf> cluster_uuid_buf = kvstore.get(
      cluster_uuid_key_space, cluster_uuid_key);
    if (cluster_uuid_buf) {
        return model::cluster_uuid{
          serde::from_iobuf<uuid_t>(std::move(*cluster_uuid_buf))};
    }
    return {};
}

ss::future<> write_stored_cluster_uuid(
  storage::kvstore& kvstore, const model::cluster_uuid& value) {
    vassert(
      ss::this_shard_id() == ss::shard_id(0),
      "Cluster UUID is only stored in shard 0");
    return kvstore.put(
      cluster_uuid_key_space, cluster_uuid_key, serde::to_iobuf(value));
}

} // namespace cluster
