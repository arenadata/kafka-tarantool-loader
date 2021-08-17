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
--- DateTime: 6/15/20 1:11 PM
---
---
local t = require('luatest')
local g = t.group('integration_adg_kafka_connector_v2.subscription')
local g2 = t.group('integration_adg_kafka_connector_v2.unsubscription')

local helper = require('test.helper.integration')
local cluster = helper.cluster
--TODO kafka in docker

g.before_each(function()
    local kafka = cluster:server('kafka_connector-1').net_box
    kafka:call('box.execute', {'truncate table _KAFKA_TOPIC'})
end)

g2.before_each(function()
    local kafka = cluster:server('kafka_connector-1').net_box
    kafka:call('box.execute', {'truncate table _KAFKA_TOPIC'})
end)

g.test_subscription_invalid_values = function()
    local kafka = cluster:server('kafka_connector-1').net_box
    t.assert_error(kafka.call,kafka, 'subscribe_to_topic', {'test_topic1', nil})
    t.assert_error(kafka.call,kafka, 'subscribe_to_topic', {nil, 100})
end

g2.test_unsubscription_invalid_values = function()
    local kafka = cluster:server('kafka_connector-1').net_box
    t.assert_error(kafka.call,kafka, 'subscribe_to_topic', {nil})
    t.assert_error(kafka.call,kafka, 'subscribe_to_topic', {100})
    t.assert_error(kafka.call,kafka, 'subscribe_to_topic', {{100}})
end