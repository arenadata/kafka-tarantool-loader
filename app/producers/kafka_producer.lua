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
--- DateTime: 6/25/20 10:37 AM
---
local checks = require('checks')
local log = require('log')
-- local misc_utils = require('app.utils.misc_utils')
local tnt_kafka = require('kafka')
-- local fiber = require('fiber')
--- Producer
---
---
local kafka_producer = {}
kafka_producer.__index = kafka_producer
kafka_producer.__call = function (cls, ...)
    return cls.new(...)
end

local function kafka_producer_init (builder)
    checks('table')
    local err
    local self = setmetatable({}, kafka_producer)
    self.broker = builder.broker
    self.options = builder.options
    self.default_topic_options = builder.default_topic_options
    self.error_callback = builder.error_callback
    self.log_callback = builder.log_callback
    self.producer, err = tnt_kafka.Producer.create({
        brokers = self.broker, -- brokers for bootstrap
        options = self.options, -- options for librdkafka
        error_callback = self.error_callback, -- optional callback for errors
        log_callback = self.log_callback, -- optional callback for logs and debug messages
        default_topic_options = self.default_topic_options })
    if err ~= nil then
        return false, err
    end
    return true, self
end

function kafka_producer.produce_async(self,topic_name, messages)
    checks('table','string','table')
    local errors = {}
    for _,v in ipairs(messages) do
        local err = self.producer:produce_async({topic = topic_name, key = v['key'], value = v['value'] })
        if err ~= nil then
            table.insert(errors,{topic = topic_name, key = v['key'], value = v['value'], error = err })
        end
    end

    if #errors > 0 then
        return false, errors
    end

    return true, nil

end

function kafka_producer.produce_sync(self,topic_name, messages)
    checks('table','string','table')
    local errors = {}
    for _,v in ipairs(messages) do
        local err = self.producer:produce({topic = topic_name, key = v['key'], value = v['value'] })
        if err ~= nil then
            table.insert(errors,{topic = topic_name, key = v['key'], value = v['value'], error = err })
        end
    end

    if #errors > 0 then
        return false, errors
    end

    return true, nil
end

function kafka_producer.close(self)
    checks('table')
    log.info("INFO: closing producer")
    local  res,err = pcall(self.producer.close,self.producer)
-- luacheck: ignore self
    self = nil
    return res,err
end

--- Builder
---
---
local kafka_producer_builder = {}
kafka_producer_builder.__index = kafka_producer_builder
kafka_producer_builder.__type = 'kafka_producer_builder'
kafka_producer_builder.__call = function (cls, ...)
    return cls.new(...)
end

function kafka_producer_builder.init(broker)
    checks('string')
    local self = setmetatable({}, kafka_producer_builder)
    self.broker = broker
    self.options = {}
    self.default_topic_options = {}
    self.error_callback = nil
    self.log_callback = nil
    return self
end

local function set_options_type(self,options,where)
    if next(options) == nil then
        error("Options must be non empty table")
    end
    for k,v in pairs(options) do
        if type(k) ~= 'string' then
            error("Options key " .. k .. " must be string")
        end
        if type(v) ~= 'string' then
            error("Options value " .. k .. ':' .. v .. " must be string")
        end
        self[where][k] = v
    end
    return self
end

function kafka_producer_builder.set_options(self,options)
    checks('kafka_producer_builder','table')
    return set_options_type(self,options,'options')
end

function kafka_producer_builder.set_default_topic_options(self,options)
    checks('kafka_producer_builder','table')
    return set_options_type(self,options,'default_topic_options')
end

function kafka_producer_builder.set_error_callback(self, error_callback)
    checks('kafka_producer_builder','function')
    self.error_callback = error_callback
    return self
end

function kafka_producer_builder.set_log_callback(self, log_callback)
    checks('kafka_producer_builder','function')
    self.log_callback = log_callback
    return self
end

function kafka_producer_builder.build(self)
    checks('kafka_producer_builder')
    return kafka_producer_init(self)
end

return kafka_producer_builder