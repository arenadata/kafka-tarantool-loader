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
--- DateTime: 6/16/20 10:12 AM
---
local checks = require('checks')
local error_repository = require('app.messages.error_repository')
local log = require('log')
local json = require('json')
local tnt_kafka = require('kafka')
local fiber = require('fiber')
local clock = require('clock')

local deserialize_transform = require('app.handlers.callback.kafka_msg_deserialize_transformation')
local insert_transform = require('app.handlers.callback.kafka_msg_insert_transformation')
local stat_insert_transform = require('app.handlers.callback.kafka_statistic_insert_transformation')
local callback_transform = require('app.handlers.callback.kafka_callback_transformation')
local error_transform = require('app.handlers.callback.kafka_error_handler_transformation')
local commit_transfrom = require('app.handlers.callback.kafka_msg_commit_transformation')
--- Consumer
---
---
local function readonlytable(table)
    return setmetatable({}, {
        __index = table,
        __newindex = function(table, key, value)
            error("Attempt to modify read-only table")
        end,
    --    __metatable = false,
        __type = 'kafka_consumer'
    });
end


local kafka_consumer = {}
kafka_consumer.__index = kafka_consumer
kafka_consumer.__call = function (cls, ...)
    return cls.new(...)
end

local function kafka_consumer_init (builder)
    checks('table')
    local err
    local self = setmetatable({}, kafka_consumer)
    self.broker = builder.broker
    self.options = builder.options
    self.default_topic_options = builder.default_topic_options
    self.error_callback = builder.error_callback
    self.log_callback = builder.log_callback
    self.rebalance_callback = builder.rebalance_callback
    self.consumer, err = tnt_kafka.Consumer.create({
            brokers = self.broker, -- brokers for bootstrap
            options = self.options, -- options for librdkafka
            error_callback = self.error_callback, -- optional callback for errors
            log_callback = self.log_callback, -- optional callback for logs and debug messages
            rebalance_callback = self.rebalance_callback,  -- optional callback for rebalance messages
            default_topic_options = self.default_topic_options })
    self.subscribe_callbacks = {}
    self.unsubscribe_callbacks = {}
    if err ~= nil then
        return false, err
    end
    return true, self
end

function kafka_consumer.pause(self,topics)
    checks('table','table')
    log.info("INFO: consumer try to pause topics " .. table.concat(topics,','))
    local err = self.consumer:pause(topics)
    if err ~= nil then
        return false,err
    end
    log.info("INFO: consumer paused topics " .. table.concat(topics,','))
    return true, nil
end

function kafka_consumer.resume(self,topics)
    checks('table', 'table')
    log.info("INFO: consumer try to resume topics " .. table.concat(topics,','))
    local err = self.consumer:resume(topics)
    if err ~= nil then
        return false, err
    end
    log.info("INFO: consumer resumed topics " .. table.concat(topics,','))
    return true, nil
end


function kafka_consumer.subscribe(self,topics)
    checks('table', 'table')
    log.info("INFO: consumer subscribing to " .. table.concat(topics,','))
    local err = self.consumer:subscribe(topics)
    if err ~= nil then
         return false,err
    end
    log.info("INFO: consumer subscribed to " .. table.concat(topics,','))
    return true, nil
end

function kafka_consumer.unsubscribe(self,topics)
    checks('table', 'table')
    log.info("INFO: consumer unsubscribing from " .. table.concat(topics,','))
    local err = self.consumer:unsubscribe(topics)
    if err ~= nil then
        return false, err
    end
    log.info("INFO: consumer unsubscribed from " .. table.concat(topics,','))
    return true, nil
end

function kafka_consumer.close(self)
    checks('table')
    log.info("INFO: closing consumer")
    local  res,err = pcall(self.consumer.close,self.consumer)
    self = nil
    return res,err
end

function kafka_consumer.commit(self,message)
    checks('table', 'userdata')
    local err = self.consumer:store_offset(message)
    if err ~= nil then
        return false, err
    end
    return true, nil
end

function kafka_consumer.commit_sync(self)
    checks('table')
    local err = self.consumer:commit_sync()
    if err ~= nil then
        return false, err
    end
    return true, nil
end

function kafka_consumer.commit_async(self)
    checks('table')
    local err = self.consumer:commit_async()
    if err ~= nil then
        return false, err
    end
    return true, nil
end

function kafka_consumer.get_message_channel(self)
    checks('table')
    local out, err = self.consumer:output()

    if err ~= nil then
        return false, err
    end

    return true, out
end

function kafka_consumer.poll_messages(self, amount)
    checks('table', 'number')
    local is_out_created,out = self:get_message_channel()

    if not is_out_created then
        return false, out
    end

    local msg_cnt = 0
    local result = {}

    local timeout = amount / 1000000

    while msg_cnt < amount do

        if out:is_closed() then
            return true,{result = result, amount = msg_cnt}
        end

        local msg = out:get(timeout)
        if msg ~= nil then
            msg_cnt = msg_cnt + 1
            table.insert(result, msg)
            else return true,{result = result, amount = msg_cnt}
        end
    end


    return true,{result = result, amount = msg_cnt}
end

function kafka_consumer.init_poll_msg_fiber(process_function,
                                            kafka_stat_space_name,
                                            kafka_stat_space_insert_function
                                            )
    checks('function','?string','?function')

    if kafka_stat_space_name ~= nil then
        if box.space[kafka_stat_space_name] == nil then
            return false, string.format("ERROR: %s space does not exists",kafka_stat_space_name)
        end

        if kafka_stat_space_insert_function == nil then
            return false, string.format("ERROR: function %s not registered in adg_kafka_connector role.",
                    kafka_stat_space_insert_function)
        end

    end

    local is_out_created,out = self:get_message_channel()

    if not is_out_created then
        return false, out
    end

    local poll_fiber = fiber.new (
        function()
            while true do
                if out.is_closed() then
                    return
                end

                local msg = out:get(0.5)
                if msg ~= nil then
                    process_function(msg)
                    local topic_name = msg:topic()
                    local partition_name = msg:partition()

                    if kafka_stat_space_name ~= nil and kafka_stat_space_insert_function ~= nil then
                        kafka_stat_space_insert_function(kafka_stat_space_name,topic_name,partition_name)
                    end

                    fiber.yield()
                end
            end
        end
    )
    poll_fiber:name("kafka_polling_fiber")
end

function kafka_consumer.init_poll_msg_fiber(self)
    local is_out_created,out = self:get_message_channel()

    if not is_out_created then
        return false, out
    end


    local e_t = error_transform.init(100)
    local error_ch = e_t:get_result_channel()

    local d_t = deserialize_transform.init(100,out,error_ch)
    local deserialized = d_t:get_result_channel()

    local i_t = insert_transform.init(100,deserialized,error_ch) -- buffer
    local inserted = i_t:get_result_channel()

    local c_t = commit_transfrom.init(100, inserted, error_ch,self)
    local committed = c_t:get_result_channel()

    local s_i_t = stat_insert_transform.init(100, committed, error_ch) -- buffer?
    local inserted_with_alert = s_i_t:get_result_channel()

    local cb_t = callback_transform.init(100, inserted_with_alert, error_ch)



end



--- Builder
---
---
local kafka_consumer_builder = {}
kafka_consumer_builder.__index = kafka_consumer_builder
kafka_consumer_builder.__type = 'kafka_consumer_builder'
kafka_consumer_builder.__call = function (cls, ...)
    return cls.new(...)
end

function kafka_consumer_builder.init(broker)
    checks('string')
    local self = setmetatable({}, kafka_consumer_builder)
    self.broker = broker
    self.options = {["group.id"] = "tnt_consumer"}
    self.default_topic_options = {}
    self.error_callback = nil
    self.log_callback = nil
    self.rebalance_callback =  nil
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

function kafka_consumer_builder.set_options(self,options)
    checks('kafka_consumer_builder','table')
    if options ~= nil and next(options) ~= nil then
        return set_options_type(self,options,'options')
    else return self
    end
end

function kafka_consumer_builder.set_default_topic_options(self,options)
    checks('kafka_consumer_builder','table')
    return set_options_type(self,options,'default_topic_options')
end


function kafka_consumer_builder.set_error_callback(self, error_callback)
    checks('kafka_consumer_builder','function')
    self.error_callback = error_callback
    return self
end

function kafka_consumer_builder.set_log_callback(self, log_callback)
    checks('kafka_consumer_builder','function')
    self.log_callback = log_callback
    return self
end

function kafka_consumer_builder.set_rebalance_callback(self, rebalance_callback)
    checks('kafka_consumer_builder','function')
    self.rebalance_callback = rebalance_callback
    return self
end

function kafka_consumer_builder.build(self)
    checks('kafka_consumer_builder')
    return kafka_consumer_init(self)
end



return kafka_consumer_builder