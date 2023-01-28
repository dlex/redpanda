# Copyright 2023 Redpanda Data, Inc.
#
# Use of this software is governed by the Business Source License
# included in the file licenses/BSL.md
#
# As of the Change Date specified in that file, in accordance with
# the Business Source License, use of this software will be governed
# by the Apache License, Version 2.0

import random, time, math
from enum import Enum
from typing import Tuple
from rptest.services.cluster import cluster
from rptest.services.redpanda import MetricsEndpoint
from rptest.tests.redpanda_test import RedpandaTest
from rptest.clients.rpk import RpkTool
from ducktape.mark import parametrize
from ducktape.tests.test import TestContext


# This file is about throughput limiting that works at shard/node/cluster (SNC)
# levels, like cluster-wide and node-wide throughput limits


class ThroughputLimitsSnc(RedpandaTest):
    """
    Throughput limiting that works at shard/node/cluster (SNC)
    levels, like cluster-wide and node-wide throughput limits
    """

    # see later if need to split into more classes

    def __init__(self, test_ctx: TestContext, *args, **kwargs):
        #self._ctx = test_ctx
        super(ThroughputLimitsSnc, self).__init__(test_ctx,
                                                  num_brokers=3,
                                                  *args,
                                                  **kwargs)
        rnd_seed = time.time()
        # self.logger.debug(f"args: {args}")
        # self.logger.debug(f"kwargs: {kwargs}")
        rnd_seed_override = test_ctx.globals.get("random_seed", True)
        if not rnd_seed_override is None:
            rnd_seed = rnd_seed_override
        # self.logger.debug(f"rs: {rs}")
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
            r = self.rnd.randrange(4)
            if r == 0:
                return None
            if r == 1:
                return 16  # practical minimum
            return math.floor(2**(self.rnd.random() * 36 + 4))  # up to 1 TB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_BPS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 30))  # up to 1 GB/s

        if prop == self.ConfigProp.QUOTA_SHARD_MIN_RATIO:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            if r == 1:
                return 1
            return self.rnd.random()

        if prop == self.ConfigProp.THROTTLE_DELAY_MAX_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 25))  # up to ~1 year

        if prop == self.ConfigProp.BAL_WINDOW_MS:
            r = self.rnd.randrange(4)
            if r == 0:
                return 1
            if r == 1:
                return 2147483647
            return math.floor(2**(self.rnd.random() * 31))

        if prop == self.ConfigProp.BAL_PERIOD_MS:
            r = self.rnd.randrange(3)
            if r == 0:
                return 0
            return math.floor(2**(self.rnd.random() * 22))  # up to ~1.5 months

        raise Exception(f"Unsupported ConfigProp: {prop}")
        #self.rpk.cluster_config_set ( prop, value )

    def setUp(self):

        self.config = {}
        for prop in list(self.ConfigProp):
            val = self.get_config_parameter_random_value(prop)
            self.config[prop] = val
            if not val is None:
                self.redpanda.add_extra_rp_conf({prop.value: val})

        self.logger.info(
            f"!SETTINGUP RP! extras:{self.redpanda._extra_rp_conf}")
        super(ThroughputLimitsSnc, self).setUp()

    def current_effective_node_quota(self) -> Tuple[int,int]:
        metrics = self.redpanda.metrics_sample(
            "quotas_snc_quota_effective",
            metrics_endpoint=MetricsEndpoint.METRICS)

        assert metrics, "Effecive quota metric is missing"
        self.logger.debug(f"Samples: {metrics.samples}")

        node_quota_in = sum(int(s.value) for s in metrics.label_filter({"direction": "ingress"}).samples)
        node_quota_eg = sum(int(s.value) for s in metrics.label_filter({"direction": "egress"}).samples)
        return node_quota_in, node_quota_eg

    @cluster(num_nodes=3)
    def test_configuration(self):
        """
        Test various configuration patterns, including extreme ones, 
        verify that it does not wreck havoc onto cluster
        """

        # TBD: parameterize to run under load or not

        self.logger.info(f"!STARTING RP! ")
        errors = 0
        run_until = time.clock_gettime(time.CLOCK_MONOTONIC) + 15
        # seconds
        while time.clock_gettime(time.CLOCK_MONOTONIC) < run_until:

            config_param = self.rnd.choice(list(self.ConfigProp))
            config_value = self.get_config_parameter_random_value(config_param)
            self.logger.debug(f"Setting {config_param.value} = {config_value}")
            self.config[config_param] = config_value
            self.rpk.cluster_config_set(config_param.value,
                                        str(config_value or ''))

            time.sleep(1)

            def check_node_quota_metric(self, config_prop: self.ConfigProp, effective_node_quota: int) ->int:
                config_val = self.config[config_prop]
                if config_val is None:
                    return 0
                expected_node_quota = config_val * len(self.redpanda.nodes)
                if effective_node_quota == expected_node_quota:
                    return 0
                self.logger.error(f"Expected quota value mismatch. Effective {effective_node_quota} != expected {expected_node_quota}. Direction: {config_prop.name}")
                return 1

            effective_node_quota = self.current_effective_node_quota()
            self.logger.debug(f"current_effective_node_quota: {effective_node_quota}")
            errors += check_node_quota_metric(self,self.ConfigProp.QUOTA_NODE_MAX_IN, effective_node_quota[0])
            errors += check_node_quota_metric(self,self.ConfigProp.QUOTA_NODE_MAX_EG, effective_node_quota[1])

        if errors != 0:
            raise Exception(f"Test has failed with {errors} errors")
