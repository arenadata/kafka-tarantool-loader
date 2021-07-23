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

local fiber = require('fiber')
local log = require('log')
local tnt_kafka = require('kafka')
local json = require('json')
local checks = require('checks')
local error_repository = require('app.messages.error_repository')

local consumer = nil

local cartridge = require('cartridge')

local kafka_message_channel_size = 1000
local message_channel = fiber.channel(kafka_message_channel_size) --TODO Buffer size in config

---resize_message_channel_size - the method, that resizes Kafka message channel size.
---@param size number - new message channel size.
local function resize_message_channel_size(size)
    checks('number')
    if kafka_message_channel_size == size then
        return
    else
        message_channel:close()
        message_channel = fiber.channel(size)
        kafka_message_channel_size = size
    end
end


---get_consumer - the method, that returns the current consumer.
---@return userdata
local function get_consumer()
    return consumer
end

---create - the method, that creates consumer.
---@param brokers string - kafka bootstrap string.
---@param options table - consumer options.
---@param additional_opts table - consumer options.
local function create(brokers, options, additional_opts)
    checks('string','?table','?table')
    local err

    local error_callback = function(error)
        error_repository.get_error_code('ADG_KAFKA_CONSUMER_001',{error = error})
    end
    local log_callback = function(fac, str, level)
        log.info("INFO: got log: %d - %s - %s", level, fac, str)
    end
    local rebalance_callback = function(msg)
        log.info("INFO: got rebalance msg: %s", json.encode(msg))
        local current_time = os.clock()
        --TODO cluster state to ensure rebalance end?

        if msg.assigned ~= nil then
            for k,v in pairs(_G.rebalance_subscriptions) do
                if current_time >= k then
                     v:signal()
                end
            end
        end

        if msg.revoked ~= nil then
            for k,v in pairs(_G.rebalance_unsubscriptions) do
                if current_time >= k then
                    v:signal()
                end
            end
        end

    end

    --Memory leak if options = {}
    if options == nil or next(options) == nil then
        options = {["group.id"] = "tnt_consumer"}
    end

    if additional_opts ~= nil then
        for key, value in pairs(additional_opts) do
            options[key] = value
        end
    end

    for key, value in pairs(options) do
        if type(key) ~= 'string' and type(value) ~= 'string' then
            options[key] = nil
            options[tostring(key)] = tostring(value)

        elseif type(key) ~= 'string' then
            options[key] = nil
            options[tostring(key)] = tostring(value)

        elseif type(value) ~= 'string' then
            options[key] = tostring(value)
        end
    end

        consumer, err = tnt_kafka.Consumer.create({
        brokers = brokers, -- brokers for bootstrap
        options = options, -- options for librdkafka
        error_callback = error_callback, -- optional callback for errors
        log_callback = log_callback, -- optional callback for logs and debug messages
        rebalance_callback = rebalance_callback,  -- optional callback for rebalance messages
         --TODO add optional default topic options
         default_topic_options = {
           -- ["auto.offset.reset"] = "earliest",
        }
    })
    if err ~= nil then
        error_repository('ADG_KAFKA_CONSUMER_005', {error=err})
        return
    end

    log.info("INFO: Consumer created")
end

---subscribe - the method, that subscribes consumer on Kafka topics.
---@param topics table - list of topics to subscribe to.
local function subscribe(topics)
    checks('table')
    if consumer ~= nil then
        log.info("INFO: consumer subscribing")

        local err = consumer:subscribe(topics)

        if err ~= nil then
            return error_repository.get_error_code('ADG_KAFKA_CONSUMER_006',{err=err})
        end
        log.info("INFO: consumer subscribed")
    else return error_repository.get_error_code('ADG_KAFKA_CONSUMER_004',{err=err})
    end
end


---unsubscribe - the method, that's unsubscribes consumer from Kafka topics.
---@param topics table - list of topics to unsubscribe to.
local function unsubscribe(topics)
    checks('table')
    log.info("INFO: consumer unsubscribing")
    if consumer ~= nil then
        local err = consumer:unsubscribe(topics)
        if err ~= nil then
            return error_repository.get_error_code('ADG_KAFKA_CONSUMER_007')
        end
        log.info("INFO: consumer unsubscribed")
    else return error_repository.get_error_code('ADG_KAFKA_CONSUMER_004')
    end
end

---get_message_from_channel - the method, that gets messages from message_channel until its empty.
---@return table - Lua table with Kafka messages.
local function get_message_from_channel()
    local consumed = {}
    while not message_channel:is_empty()
     do
        local res = message_channel:get(120)
        if res ~= nil then
            table.insert(consumed,res)
        end
    end
    return consumed
end


---poll - the method, that gets messages from Kafka and puts them on message channel
---while the consumer is alive.
local function poll()

    if consumer ~= nil then

        local out, err = consumer:output()

        if err ~= nil then
            error_repository.get_error_code('ADG_KAFKA_CONSUMER_002', {error = err})
        end

        while true
        do
            if message_channel:is_full() or message_channel:is_closed() or not cartridge.is_healthy() then
                fiber.sleep(0.5)
            end

            if out:is_closed() then
                return
            end

            if not message_channel:is_full() and not message_channel:is_closed() and cartridge.is_healthy() then
                local msg = out:get()
                if msg ~= nil then
                    local is_message_added = message_channel:put(msg,120)
                    if not is_message_added then
                        error_repository.get_error_code('ADG_KAFKA_CONSUMER_009',
                                {timeout=120,channel_size = kafka_message_channel_size})
                        fiber.sleep(0.5)
                        -- out:put?
                    end
                end
            else fiber.sleep(0.5)
            end
        end
    else error_repository.get_error_code('ADG_KAFKA_CONSUMER_004')
    end
end

---commit - the method, that's commit array of Kafka messages in Kafka.
---@param last_messages table - messages to commit.
---@return boolean|string - true|nil if commit successful, else false|error msg
local function commit(last_messages)
    checks('table')
    if consumer ~= nil then
        for _,v in ipairs(last_messages) do
            local err = consumer:store_offset(v) --TODO SAVE in metadata table?
            if err ~= nil then
                return false,error_repository.get_error_code('ADG_KAFKA_CONSUMER_003', {error = err, topic = v:topic()})
            end
        end
        return true,nil
    else
        return false,error_repository.get_error_code('ADG_KAFKA_CONSUMER_004')
    end
end

---close - the method, that's destroy kafka consumer and free resources.
local function close()
    if consumer ~= nil then
        log.info("INFO: closing consumer")
        local  _,err = consumer:close()
        if err ~= nil then
            error_repository.get_error_code('ADG_KAFKA_CONSUMER_008',{error=err})
        end
    end
    consumer = nil
end

return {
    create = create,
    subscribe = subscribe,
    unsubscribe = unsubscribe,
    close = close,
    commit = commit,
    poll = poll,
    get_message_from_channel = get_message_from_channel,
    get_consumer = get_consumer,
    resize_message_channel_size = resize_message_channel_size
}