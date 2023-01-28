// Copyright 2023 Redpanda Data, Inc.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.md
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0

#include "kafka/server/snc_quota_manager.h"

#include <boost/test/unit_test.hpp>

namespace kafka {
int64_t cap_to_ceiling(int64_t& value, const int64_t limit);
}

BOOST_AUTO_TEST_CASE(cap_to_ceiling_test) {
    int64_t v;
    BOOST_CHECK_EQUAL(kafka::cap_to_ceiling(v=0,0), 0);
    BOOST_CHECK_EQUAL(v,0);
    BOOST_CHECK_EQUAL(kafka::cap_to_ceiling(v=100,0), 0);
    BOOST_CHECK_EQUAL(v,100);
    BOOST_CHECK_EQUAL(kafka::cap_to_ceiling(v=0,110), 110);
    BOOST_CHECK_EQUAL(v,110);
    BOOST_CHECK_EQUAL(kafka::cap_to_ceiling(v=-53,0), 53);
    BOOST_CHECK_EQUAL(v,0);
    BOOST_CHECK_EQUAL(kafka::cap_to_ceiling(v=-4110014,-(-10725196)), 10725196+4110014);
    BOOST_CHECK_EQUAL(v,10725196);
}
