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
--- DateTime: 5/7/20 7:14 PM
---
local t = require('luatest')
local g = t.group('integration_adg_kafka_connector.config')

local helper = require('test.helper.integration')
local cluster = helper.cluster

g.before_each(function()
    cluster:upload_config(helper.cluster_config)
end)

g.test_kafka_bootstrap_validation_errors = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy['kafka_bootstrap']['bootstrap_connection_string'] = nil
    t.assert_error(cluster.upload_config,cluster,config_copy)
    config_copy['kafka_bootstrap']['bootstrap_connection_string123'] = '123'
    t.assert_error(cluster.upload_config,cluster,config_copy)
    config_copy['kafka_bootstrap']['bootstrap_connection_string'] = 123
    t.assert_error(cluster.upload_config,cluster,config_copy)
end

g.test_kafka_bootstrap_validation_success = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy['kafka_bootstrap']['bootstrap_connection_string'] = 'localhost:9092'
    local res = cluster:upload_config(config_copy)
    t.assert_items_include(res,{reason='Ok'})
end

g.test_topics_validation_errors_1 = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    table.insert(config_copy['kafka_consume']['topics'], 'TEST_TOPIC')

    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['error_topic'] = 'ERR_TOPIC',
        ['success_topic'] = 'SUCC_TOPIC'

    }
    t.assert_error(cluster.upload_config,cluster,config_copy)


    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['error_topic'] = 'ERR_TOPIC',
        ['success_topic'] = 'SUCC_TOPIC'
    }

    t.assert_error(cluster.upload_config,cluster,config_copy)


    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['error_topic'] = 'ERR_TOPIC',
        ['success_topic'] = 'SUCC_TOPIC'
    }

    t.assert_error(cluster.upload_config,cluster,config_copy)


    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['success_topic'] = 'SUCC_TOPIC'
    }

    t.assert_error(cluster.upload_config,cluster,config_copy)

    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['error_topic'] = 'ERR_TOPIC'
    }

    t.assert_error(cluster.upload_config,cluster,config_copy)

end

g.test_topics_validation_errors_2 = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    table.insert(config_copy['kafka_consume']['topics'], 'TEST_TOPIC2')

    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['error_topic'] = 'ERR_TOPIC',
        ['success_topic'] = 'SUCC_TOPIC'

    }

    t.assert_error(cluster.upload_config,cluster,config_copy)
end

g.test_topics_validation_success = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    table.insert(config_copy['kafka_consume']['topics'], 'TEST_TOPIC')

    config_copy['kafka_topics']['TEST_TOPIC'] = {
        ['target_table'] = 'TEST_TABLE',
        ['schema_key'] = 'TEST_TABLE_KEY',
        ['schema_data'] = 'TEST_TABLE_DATA',
        ['error_topic'] = 'ERR_TOPIC',
        ['success_topic'] = 'SUCC_TOPIC'

    }
    local res = cluster:upload_config(config_copy)
    t.assert_items_include(res,{reason='Ok'})
end

g.test_consumer_option_validation_success = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy['kafka_consume']['properties']['enable.auto.offset.store'] = 'true'
    local res = cluster:upload_config(config_copy)
    t.assert_items_include(res,{reason='Ok'})
end

g.test_consumer_option_validation_errors = function()
    local config_copy = table.deepcopy(helper.cluster_config)
    config_copy['kafka_consume']['properties']['enable.auto.offset.store'] = true
    t.assert_error(cluster.upload_config,cluster,config_copy)

    config_copy['kafka_consume']['properties']['enable.auto.offset.store'] = 123
    t.assert_error(cluster.upload_config,cluster,config_copy)

    config_copy['kafka_consume']['properties']['enable.auto.offset.store'] = nil
    config_copy['kafka_consume']['properties'][123] = "123"
    t.assert_error(cluster.upload_config,cluster,config_copy)
end