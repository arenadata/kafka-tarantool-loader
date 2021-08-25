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

local tnt_kafka = require("kafka")
local log = require("log")
local misc_utils = require("app.utils.misc_utils")
local fiber = require("fiber")
local checks = require("checks")

local producer = nil
local errors = {}
local logs = {}

local function get_producer()
    return producer
end

local function create(brokers, options, additional_opts)
    checks("string", "table", "table")
    local err
    errors = {}
    logs = {}

    local error_callback = function(error)
        log.error("ERROR: got error: %s", error)
    end
    local log_callback = function(fac, str, level)
        log.info("INFO: got log: %d - %s - %s", level, fac, str)
    end

    if additional_opts ~= nil then
        for key, value in pairs(additional_opts) do
            options[key] = value
        end
    end

    for key, value in pairs(options) do
        if type(key) ~= "string" and type(value) ~= "string" then
            options[key] = nil
            options[tostring(key)] = tostring(value)
        elseif type(key) ~= "string" then
            options[key] = nil
            options[tostring(key)] = tostring(value)
        elseif type(value) ~= "string" then
            options[key] = tostring(value)
        end
    end

    producer, err = tnt_kafka.Producer.create({
        brokers = brokers,
        options = options,
        log_callback = log_callback,
        error_callback = error_callback,
        default_topic_options = {
            ["partitioner"] = "murmur2_random",
        },
    })
    if err ~= nil then
        log.error("ERROR: got err %s", err)
    end
end

local function produce_messages(topic_name, messages, is_async)
    checks("string", "table", "boolean") --TODO normal message check
    if producer ~= nil then
        local err
        for _, v in ipairs(messages) do
            if is_async == false then
                err = producer:produce({ topic = topic_name, key = v["key"], value = v["value"] })
            else
                err = producer:produce_async({ topic = topic_name, key = v["key"], value = v["value"] })
            end
            if err ~= nil then
                log.error("ERROR: got error '%s' while sending key:value '%s:%s", err, v["key"], v["value"])
                return false, err
            else
                log.info("INFO: successfully sent key:value '%s:%s'", v["key"], v["value"])
            end
        end
        return true, nil
    end
end

local function produce(topic_name, messages, opts)
    checks(
        "string",
        "table", --TODO normal message check
        {
            is_async = "?boolean",
            sync_fibers_cnt = "?number",
        }
    )

    if producer ~= nil then
        if opts["is_async"] == nil then
            opts["is_async"] = false
        end

        if opts["sync_fibers_cnt"] == nil then
            opts["sync_fibers_cnt"] = 1
        end
        if opts["is_async"] == true or (opts["is_async"] == false and opts["sync_fibers_cnt"] == 1) then --TODO Fix Error
            local res, err = produce_messages(topic_name, messages, opts["is_async"])
            return res, err
        else
            local splitted_messages = misc_utils.split_table_in_chunks(messages, opts["sync_fibers_cnt"])
            for i = 1, opts["sync_fibers_cnt"] do
                fiber.create(produce_messages, topic_name, splitted_messages[i], opts["is_async"])
            end
        end
    end
end

local function get_errors()
    return errors
end

local function get_logs()
    return logs
end

local function close()
    if producer ~= nil then
        local _, err = producer:close()
        if err ~= nil then
            log.error("ERROR: got err %s", tostring(err))
        end
    end
    producer = nil
end

return {
    create = create,
    produce = produce,
    get_errors = get_errors,
    get_logs = get_logs,
    close = close,
    get_producer = get_producer,
}
