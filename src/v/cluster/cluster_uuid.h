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

#pragma once

#include "bytes/bytes.h"
#include "model/fundamental.h"
#include "storage/fwd.h"
#include "utils/uuid.h"

namespace cluster {

/**
 * Read the stored cluster uuid from the kvstore
 * \return Cluster UUID if stored in kvstore, empty otherwise
 */
std::optional<model::cluster_uuid> read_stored_cluster_uuid(storage::kvstore&);

ss::future<>
write_stored_cluster_uuid(storage::kvstore&, const model::cluster_uuid&);

} // namespace cluster
