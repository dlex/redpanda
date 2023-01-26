# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random, time, math
from rptest.services.cluster import cluster
from rptest.tests.redpanda_test import RedpandaTest
from enum import Enum
from rptest.clients.rpk import RpkTool

# This file is about throughput limiting that works at shard/node/cluster (SNC)
# levels, like cluster-wide and node-wide throughput limits


class ThroughputLimitsSnc(RedpandaTest):
    """
    Throughput limiting that works at shard/node/cluster (SNC)
    levels, like cluster-wide and node-wide throughput limits
    """

    # see later if need to split into more classes

    def __init__(self, test_ctx, *args, **kwargs):
        self._ctx = test_ctx
        super(ThroughputLimitsSnc, self).__init__(test_ctx,
                                                  num_brokers=3,
                                                  *args,
                                                  **kwargs)
        rnd_seed = time.time()
        self.logger.info(f"Random seed: {rnd_seed}")
        self.rnd = random.Random(rnd_seed)
        self.rpk = RpkTool(self.redpanda)

    class ConfigProp(Enum):
        QUOTA_NODE_MAX_IN = "kafka_throughput_limit_node_in_bps"
        QUOTA_NODE_MAX_EG = "kafka_throughput_limit_node_out_bps"
        THROTTLE_DELAY_MAX_MS = "max_kafka_throttle_delay_ms"
        BAL_WINDOW_MS = "kafka_quota_balancer_window_ms"
        BAL_PERIOD_MS = "kafka_quota_balancer_node_period_ms"
        QUOTA_SHARD_MIN_RATIO = "kafka_quota_balancer_min_shard_thoughput_ratio"
        QUOTA_SHARD_MIN_BPS = "kafka_quota_balancer_min_shard_thoughput_bps"

    def get_config_parameter_random_value(self, prop: ConfigProp):

        if prop in [
                self.ConfigProp.QUOTA_NODE_MAX_IN,
                self.ConfigProp.QUOTA_NODE_MAX_EG
        ]:
            r = self.rnd.randrange(3)
            if r == 0:
                return None
            if r == 1:
                return 1
            return self.rnd.randrange(1, 1000)  # TBD calibrate

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_BPS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return 1
            if r == 2:
                return self.rnd.randrange(1, 1000)  # TBD calibrate

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_RATIO:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return self.rnd.random()
            if r == 2:
                return 1

        if prop == self.ConfigProp.THROTTLE_DELAY_MAX_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return math.floor(math.ldexp(
                    2, self.rnd.randrange(32)))  # TBD calibrate
            if r == 2:
                return 1001

        if prop == self.ConfigProp.BAL_WINDOW_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return math.floor(math.ldexp(
                    2, self.rnd.randrange(32)))  # TBD calibrate
            if r == 2:
                return 1001

        if prop == self.ConfigProp.BAL_PERIOD_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return math.floor(math.ldexp(
                    2, self.rnd.randrange(32)))  # TBD calibrate
            if r == 2:
                return 1001

        raise Exception(f"Unsupported ConfigProp: {prop}")
        #self.rpk.cluster_config_set ( prop, value )

    def setUp(self):

        for prop in list(self.ConfigProp):
            val = self.get_config_parameter_random_value(prop)
            if not val is None:
                self.redpanda.add_extra_rp_conf({prop.value: val})

        self.logger.info(
            f"!SETTINGUP RP! extras:{self.redpanda._extra_rp_conf}")
        super(ThroughputLimitsSnc, self).setUp()

    @cluster(num_nodes=4)
    def test_configuration(self):
        """
        Test various configuration patterns, including extreme ones, 
        verify that it does not wreck havoc onto cluster
        """

        # start a cluster
        # create a topic with ? partitions
        # start a permanently working producer and consumer
        # ensure the consumer keeps receiving the messages all the time
        # change cluster config in many ways

        self.logger.info(f"!STARTING RP!")

        run_until = time.clock_gettime(time.CLOCK_MONOTONIC) + 15
        # seconds
        while time.clock_gettime(time.CLOCK_MONOTONIC) < run_until:

            config_param = self.rnd.choice(list(self.ConfigProp))
            config_value = self.get_config_parameter_random_value(config_param)
            self.logger.debug(f"Setting {config_param.value} = {config_value}")
            self.rpk.cluster_config_set(config_param.name,
                                        str(config_value or ''))

            time.sleep(1)
