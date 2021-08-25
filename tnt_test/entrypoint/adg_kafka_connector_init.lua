#!/usr/bin/env tarantool
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

require("strict").on()
local checks = require("checks")

-- mock kafka_producer
local kafka_producer_builder = require("app.producers.kafka_producer")
local kafka_producer = {}
kafka_producer.__index = kafka_producer
kafka_producer.__call = function(cls, ...)
    return cls.new(...)
end

local function kafka_producer_init(builder)
    checks("table")
    local self = setmetatable({}, kafka_producer)
    return true, self
end

function kafka_producer.produce_async(self, topic_name, messages)
    return nil, "error from mock test"
end

kafka_producer_builder["build"] = function(self)
    checks("kafka_producer_builder")
    return kafka_producer_init(self)
end
package.preload["app.producers.kafka_producer"] = function()
    return kafka_producer_builder
end

local kafka_consumer_builder = require("app.consumers.kafka_consumer")
function kafka_consumer_builder.get_message_from_channel()
    local inner = {}
    function inner.topic(self)
        return 1
    end
    function inner.partition(self)
        return 1
    end
    function inner.offset(self)
        return 1
    end
    function inner.key(self)
        return 1
    end
    function inner.value(self)
        return 1
    end
    return { inner }
end
package.preload["app.consumers.kafka_consumer"] = function()
    return kafka_consumer_builder
end

_G.insert_messages_from_kafka = nil
local function insert_messages_from_kafka(
    messages,
    parse_key_function_str,
    parse_value_function_str,
    spaces,
    avro_schema
)
    print("-------insert_messages_from_kafka--------")
    print(require("json").encode(messages))
    print("---------------")
    if messages ~= nil then
        return nil
    end
    return true
end
package.preload["fake_adg_input_processor"] = function()
    return {
        role_name = "app.roles.adg_input_processor",
        init = function()
            _G.insert_messages_from_kafka = insert_messages_from_kafka
        end,
        insert_messages_from_kafka = insert_messages_from_kafka,
        dependencies = { "cartridge.roles.vshard-router" },
    }
end

package.setsearchroot(".")

local cartridge = require("cartridge")
local ok, err = cartridge.cfg({
    workdir = "tmp/db",
    roles = {
        "cartridge.roles.vshard-storage",
        "cartridge.roles.vshard-router",
        "app.roles.adg_api",
        "app.roles.adg_storage",
        "app.roles.adg_kafka_connector",
        "app.roles.adg_scheduler",
        "fake_adg_input_processor",
        "app.roles.adg_output_processor",
        "app.roles.adg_state",
    },
    cluster_cookie = "memstorage-cluster-cookie",
})

assert(ok, tostring(err))
