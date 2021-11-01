-- Copyright 2021 Kafka-Tarantool-Loader
--
-- Licensed under the Apache License, Version 2.0 (the "License");
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--     http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.

---
--- Created by Igor Kuznetsov.
--- DateTime: 5/10/21 12:13
---
local t = require("luatest")
local g1 = t.group("hash_calculation")

local helper = require("test.helper.integration")
local cluster = helper.cluster


g1.test_new_query = function()
    local api = cluster:server("master-1-1").net_box

    -- luacheck: max line length 200
    local r, err = api:eval('local route_utils = require("app.utils.route_utils"); return route_utils.get_bucket_id("EMPLOYEES_HOT", { 1, 1, "123", "123", "123", 100, 0, 100 }, 30000)')
    t.assert_equals(err, nil)
    t.assert_equals(r, 3939)

    -- luacheck: max line length 200
    r, err = api:eval('local route_utils = require("app.utils.route_utils"); return route_utils.get_bucket_id("hash_testing", { 1, "222", 10, 1 }, 30000)')
    t.assert_equals(err, nil)
    t.assert_equals(r, 2926)

    -- luacheck: max line length 200
    r, err = api:eval('local route_utils = require("app.utils.route_utils"); return route_utils.get_bucket_id("hash_testing", { 100, "тесты", 10, 1 }, 30000)')
    t.assert_equals(err, nil)
    t.assert_equals(r, 17338)
end

