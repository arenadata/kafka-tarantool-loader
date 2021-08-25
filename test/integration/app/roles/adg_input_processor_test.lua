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
--- Created by ashitov.
--- DateTime: 5/12/20 2:40 PM
---
local t = require("luatest")
local g = t.group("integration_adg_input_processor.config")

local helper = require("test.helper.integration")
local cluster = helper.cluster

g.before_each(function()
    cluster:upload_config(helper.cluster_config)
end)

g.test_schema_registry_options_errors = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy["kafka_schema_registry"]["host"] = 123
    t.assert_error(cluster.upload_config, cluster, config_copy)
    config_copy["kafka_schema_registry"]["port"] = "abcd"
    t.assert_error(cluster.upload_config, cluster, config_copy)
end

g.test_schema_registry_options_errors2 = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy["kafka_schema_registry"]["host"] = nil
    t.assert_error(cluster.upload_config, cluster, config_copy)
end

g.test_schema_registry_options_errors3 = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy["kafka_schema_registry"]["port"] = nil
    t.assert_error(cluster.upload_config, cluster, config_copy)
end

g.test_schema_registry_options_success = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy["kafka_schema_registry"]["host"] = "localhost"
    config_copy["kafka_schema_registry"]["port"] = 8081
    local res = cluster:upload_config(config_copy)
    t.assert_items_include(res, { reason = "Ok" })
end
