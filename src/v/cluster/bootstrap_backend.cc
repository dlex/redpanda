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

#include "cluster/bootstrap_backend.h"

#include "cluster/commands.h"
#include "cluster/logger.h"
#include "cluster/types.h"
#include "security/credential_store.h"

namespace cluster {

bootstrap_backend::bootstrap_backend(
  ss::sharded<security::credential_store>& credentials,
  ss::sharded<storage::api>& storage)
  : _credentials(credentials)
  , _storage(storage) {}

namespace {

std::error_code
do_apply(user_and_credential cred, security::credential_store& store) {
    if (store.contains(cred.username)) {
        return errc::user_exists;
    }
    store.put(cred.username, std::move(cred.credential));
    return errc::success;
}

template<typename Cmd>
ss::future<std::error_code> dispatch_updates_to_cores(
  Cmd cmd, ss::sharded<security::credential_store>& sharded_service) {
    auto res = co_await sharded_service.map_reduce0(
      [cmd](security::credential_store& service) {
          return do_apply(std::move(cmd), service);
      },
      std::optional<std::error_code>{},
      [](std::optional<std::error_code> result, std::error_code error_code) {
          if (!result.has_value()) {
              result = error_code;
          } else {
              vassert(
                result.value() == error_code,
                "State inconsistency across shards detected, "
                "expected result: {}, have: {}",
                result->value(),
                error_code);
          }
          return result.value();
      });
    co_return res.value();
}

} // namespace

ss::future<std::error_code>
bootstrap_backend::apply_update(model::record_batch b) {
    vlog(clusterlog.info, "Applying update to bootstrap_manager");

    // handle node managements command
    static constexpr auto accepted_commands
      = make_commands_list<bootstrap_cluster_cmd>();
    auto cmd = co_await cluster::deserialize(std::move(b), accepted_commands);

    co_return co_await ss::visit(
      cmd, [this](bootstrap_cluster_cmd cmd) -> ss::future<std::error_code> {
          if (_storage.local().get_cluster_uuid().has_value()) {
              vlog(
                clusterlog.debug,
                "Skipping bootstrap_cluster_cmd {}, current cluster_uuid: {}",
                cmd.value.uuid,
                *_storage.local().get_cluster_uuid());
              co_return errc::cluster_already_exists;
          }

          if (cmd.value.bootstrap_user_cred) {
              const std::error_code res
                = co_await dispatch_updates_to_cores<user_and_credential>(
                  *cmd.value.bootstrap_user_cred, _credentials);
              if (!res) {
                  vlog(
                    clusterlog.warn,
                    "Failed to dispatch bootstrap user: {}",
                    res);
                  co_return res;
              }
              vlog(
                clusterlog.info,
                "Bootstrap user {} created",
                cmd.value.bootstrap_user_cred->username);
          }

          co_await _storage.invoke_on_all(
            [&new_cluster_uuid = cmd.value.uuid](storage::api& storage) {
                storage.set_cluster_uuid(new_cluster_uuid);
                return ss::make_ready_future();
            });

          co_await write_stored_cluster_uuid(
            _storage.local().kvs(), cmd.value.uuid);
          vlog(clusterlog.info, "Cluster created {}", cmd.value.uuid);
          co_return errc::success;
      });
}

const bytes cluster_uuid_key = "cluster_uuid";
const storage::kvstore::key_space cluster_uuid_key_space
  = storage::kvstore::key_space::controller;
constexpr ss::shard_id cluster_uuid_shard = ss::shard_id(0);

/*static*/ std::optional<model::cluster_uuid>
bootstrap_backend::read_stored_cluster_uuid(storage::kvstore& kvstore) {
    vassert(
      ss::this_shard_id() == cluster_uuid_shard,
      "Cluster UUID is only stored in shard 0");
    std::optional<iobuf> cluster_uuid_buf = kvstore.get(
      cluster_uuid_key_space, cluster_uuid_key);
    if (cluster_uuid_buf) {
        return model::cluster_uuid{
          serde::from_iobuf<uuid_t>(std::move(*cluster_uuid_buf))};
    }
    return {};
}

/*static*/ ss::future<> bootstrap_backend::write_stored_cluster_uuid(
  storage::kvstore& kvstore, const model::cluster_uuid& value) {
    vassert(
      ss::this_shard_id() == cluster_uuid_shard,
      "Cluster UUID is only stored in shard 0");
    return kvstore.put(
      cluster_uuid_key_space, cluster_uuid_key, serde::to_iobuf(value));
}

} // namespace cluster
