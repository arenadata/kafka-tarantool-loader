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
--- Generated by EmmyLua(https://github.com/EmmyLua)
--- Created by ashitov.
--- DateTime: 8/4/20 12:28 PM
---
local fiber = require("fiber")
local log = require("log")
local clock = require("clock")
local cartridge = require("cartridge")

local kafka_callback_transformation = {}
kafka_callback_transformation.__index = kafka_callback_transformation
kafka_callback_transformation.__type = "end_transformation"
kafka_callback_transformation.__call = function(cls, ...)
    return cls.new(...)
end

local function set_callback_result_in_logs(log_id, is_success, error_message, error_dest, msg, fiber_name)
    local _, err = cartridge.rpc_call(
        "app.roles.adg_state",
        "set_kafka_callback_log_result",
        { log_id, is_success, error_message }
    )

    if err ~= nil then
        log.error(err)
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = err,
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        return nil
    end
end
local function register_callback_call_in_logs(msg, error_dest, fiber_name)
    local topic_name = msg.msg.topic

    if topic_name == nil then
        log.error("ERROR: Cannot find topic name in kafka message")
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = "ERROR: Cannot find topic name in kafka message",
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        return nil
    end

    local partition_name = msg.msg.partition

    if partition_name == nil then
        log.error("ERROR: Cannot find partition name in kafka message")
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = "ERROR: Cannot find partition name in kafka message",
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        return nil
    end

    local function_name = msg.msg.cb_func_name

    if function_name == nil then
        log.error("ERROR: Cannot find callback function name in kafka message")
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = "ERROR: Cannot find callback function name in kafka message",
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        return nil
    end

    local log_id, err = cartridge.rpc_call(
        "app.roles.adg_state",
        "insert_kafka_callback_log",
        { topic_name, tostring(partition_name), function_name }
    )

    if err ~= nil then
        log.error(err)
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = err,
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        return nil
    end

    return log_id
end

local function process(msg, error_dest, fiber_name)
    local callback_function_name = msg.msg.cb_func_name

    if callback_function_name == nil then
        return nil
    end

    local callback_function_params = msg.msg.cb_func_params

    local log_id = register_callback_call_in_logs(msg, error_dest, fiber_name)

    local _, err = cartridge.rpc_call("app.roles.adg_api", callback_function_name, { callback_function_params })

    if err ~= nil then
        log.error(err)
        if error_dest ~= nil then
            error_dest:put({
                msg = msg.msg,
                err = err,
                fiber = fiber_name,
                timestamp = clock.time(),
            })
        end
        if log_id ~= nil then
            set_callback_result_in_logs(log_id, false, err, error_dest, msg, fiber_name)
        end
        return nil
    end

    if log_id ~= nil then
        set_callback_result_in_logs(log_id, true, nil, error_dest, msg, fiber_name)
    end
end

local function create_fiber(self, process_function, fiber_name, source, error_dest)
    local process_fiber = fiber.new(function()
        while true do
            if source:is_closed() or self.process_channel:is_closed() then
                log.warn("WARN: Channels closed in fiber %s", fiber_name)
                return
            end
            local input = source:get()
            if input ~= nil then
                local result = process_function(input, error_dest, fiber_name)
                if result ~= nil then
                    local sent = true
                    if not sent then
                        log.error("ERROR: %s fiber send error", fiber_name)
                        if error_dest ~= nil then
                            error_dest:put({
                                msg = result.msg,
                                err = string.format("ERROR: %s fiber send error", fiber_name),
                                fiber = fiber_name,
                                timestamp = clock.time(),
                            })
                        end
                    end
                end
            end
            fiber.yield()
        end
    end)
    process_fiber:name(fiber_name)
    return process_fiber
end

function kafka_callback_transformation.init(channel_capacity, source, error_dest)
    local self = setmetatable({}, kafka_callback_transformation)
    self.process_channel = fiber.channel(channel_capacity)
    self.process_fiber = create_fiber(self, process, "kafka_сallback_fiber", source, error_dest)
    return self
end

function kafka_callback_transformation.get_result_channel(self)
    return self.process_channel
end

return kafka_callback_transformation
