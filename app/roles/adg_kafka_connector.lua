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

local log = require("log")
local checks = require("checks")
local kafka_consumer = require("app.consumers.kafka_consumer")
local kafka_producer = require("app.producers.kafka_producer")
local kafka_utils = require("app.utils.kafka_utils")
local config_utils = require("app.utils.config_utils")
local route_utils = require("app.utils.route_utils")
local schema_utils = require("app.utils.schema_utils")
local fiber = require("fiber")
local cartridge = require("cartridge")
local prometheus = require("metrics.plugins.prometheus")
local fun = require("fun")
local error_repository = require("app.messages.error_repository")
local success_repository = require("app.messages.success_repository")
local validate_utils = require("app.utils.validate_utils")
local json = require("json")
local vshard = require("vshard")
local metrics = require("app.metrics.metrics_storage")
local errors = require("errors")
local yaml = require("yaml")
local role_name = "app.roles.adg_kafka_connector"
local last_topics = {} -- luacheck: ignore last_topics
local garbage_fiber = nil
local is_consumer_config_change = false -- luacheck: ignore is_consumer_config_change
local is_producer_config_change = false -- luacheck: ignore is_producer_config_change
local producer = nil
local default_consumer = nil
local err_storage = errors.new_class("Kafka storage error")
local topic_x_consumers = {}
local cartridge_rpc = require("cartridge.rpc")

_G.get_messages_from_kafka = nil
_G.send_messages_to_kafka = nil
_G.get_metric = nil
_G.subscribe_to_topic = nil
_G.unsubscribe_from_topic = nil
_G.dataload_from_topic = nil
_G.rebalance_subscriptions = {}
_G.rebalance_unsubscriptions = {}

local function stop()
    garbage_fiber:cancel()
    return true
end

local function validate_config(conf_new, conf_old)
    error_repository.init_error_repo("en")
    success_repository.init_success_repo("en")
    if type(box.cfg) == "table" and not box.cfg.read_only then
        if conf_old ~= nil then
            last_topics = config_utils.get_all_topics(conf_old)
        end

        local kafka_bootstrap = yaml.decode(
            conf_new["kafka_bootstrap.yml"] or [[bootstrap_connection_string: "localhost:9092"]]
        )
        local is_bootstrap_config_ok, bootstrap_config_err = validate_utils.check_kafka_bootstrap_definition(
            kafka_bootstrap
        )

        if not is_bootstrap_config_ok then
            return false, bootstrap_config_err
        end

        local kafka_topics = yaml.decode(conf_new["kafka_topics.yml"] or [[]]) or {}
        local kafka_consumers = yaml.decode(conf_new["kafka_consume.yml"] or [[]])
            or { ["topics"] = {}, ["properties"] = {}, ["custom_properties"] = {} }
        local kafka_producers = yaml.decode(conf_new["kafka_produce.yml"] or [[]])
            or { ["properties"] = {}, ["custom_properties"] = {} }

        local is_topic_defs_ok, topic_defs_err = validate_utils.check_topic_definition(
            kafka_consumers["topics"],
            kafka_topics
        )

        if not is_topic_defs_ok then
            return false, topic_defs_err
        end

        local options_to_check = {
            kafka_consumers["properties"],
            kafka_consumers["custom_properties"],
            kafka_producers["properties"],
            kafka_producers["custom_properties"],
        }

        local is_options_defs_ok, options_defs_err = validate_utils.check_options_for_type(options_to_check, "string")

        if not is_options_defs_ok then
            return false, options_defs_err
        end

        if conf_old["kafka_bootstrap.yml"] ~= conf_new["kafka_bootstrap.yml"] then
            is_consumer_config_change = true
            is_producer_config_change = true
        elseif
            conf_old["kafka_consume.yml"] ~= conf_new["kafka_consume.yml"]
            and conf_old["kafka_produce.yml"] ~= conf_old["kafka_produce.yml"]
        then
            is_consumer_config_change = true
            is_producer_config_change = true
        elseif conf_old["kafka_consume.yml"] ~= conf_new["kafka_consume.yml"] then
            is_consumer_config_change = true
            is_producer_config_change = false
        elseif conf_old["kafka_produce.yml"] ~= conf_old["kafka_produce.yml"] then
            is_producer_config_change = true
            is_consumer_config_change = false
        else
            is_consumer_config_change = false
            is_producer_config_change = false
        end
    end
    return true
end

local function apply_config(conf, opts)
    -- luacheck: no unused args
    if opts.is_master and pcall(vshard.storage.info) == false then
        schema_utils.drop_all()
        if conf.schema ~= nil then -- luacheck: ignore 542
        end
    end
    kafka_utils.init_kafka_opts()
    route_utils.init_routes()

    return true
end

local function extract_last_messages_from_batch(batch)
    --TODO is channel ordered?
    checks("table")
    local result = {}

    for _, v in ipairs(batch) do
        local current_topic = v:topic()
        local current_partition = v:partition()
        local key = current_topic .. "_" .. current_partition
        local current_offset = v:offset()
        if result[key] == nil then
            result[key] = v
        else
            if current_offset > result[key]:offset() then
                result[key] = v
            end
        end
    end
    return result
end

local function serialize_kafka_messages(batch)
    checks("table")
    local result = {}
    for _, v in ipairs(batch) do
        table.insert(
            result,
            { topic = v:topic(), partition = v:partition(), offset = v:offset(), key = v:key(), value = v:value() }
        )
    end
    return result
end

local function get_messages_from_kafka()
    while true do
        if not cartridge.is_healthy() then
            fiber.sleep(5)
        end

        local batch = kafka_consumer.get_message_from_channel()
        if #batch > 0 then
            local last_messages = extract_last_messages_from_batch(batch)
            log.info("INFO: Sending data from kafka_connector to input processor")

            local serialized_kafka_msgs = serialize_kafka_messages(batch)

            local return_values = cartridge.rpc_call(
                "app.roles.adg_input_processor",
                "insert_messages_from_kafka",
                { serialized_kafka_msgs, "parse_avro", "parse_avro" }
            )

            if return_values == nil then
                --TODO Send messages to kafka
                for _, v in ipairs(serialized_kafka_msgs) do
                    metrics.kafka_messages_total_counter:inc(1, { status = "error" })
                    -- luacheck: ignore send_messages_to_kafka
                    local rpc_send_ok_ok, rpc_send_ok_err = send_messages_to_kafka(
                        route_utils.get_topic_error()[v["topic"]],
                        {
                            key = v["key"],
                            value = error_repository.get_error_code("ADG_KAFKA_CONNECTOR_002", {
                                source = v["topic"],
                                destination = "Unknown",
                            }),
                        },
                        { is_async = true }
                    )
                    if not rpc_send_ok_ok then
                        log.error(rpc_send_ok_err)
                        return nil, rpc_send_ok_err
                    end
                end
                --TODO Error check
            end

            local valid_messages, not_valid_messages = fun.partition(
                -- luacheck: ignore k
                function(k, v)
                    return v["result"] == true
                end,
                return_values
            )

            if fun.length(not_valid_messages) > 0 then
                log.info("Sending errors to kafka")

                for k, v in pairs(fun.tomap(not_valid_messages)) do
                    metrics.kafka_messages_total_counter:inc(1, { status = "error" })
                    local kafka_msg_errors = json.decode(v["error"])
                    kafka_msg_errors.opts["source"] = v["topic"]
                    kafka_msg_errors.opts["destination"] = v["space"]
                    -- luacheck: ignore send_messages_to_kafka
                    local rpc_send_ok_ok, rpc_send_ok_err = send_messages_to_kafka(
                        route_utils.get_topic_error()[v["topic"]],
                        { [1] = { key = v["key"], value = json.encode(kafka_msg_errors) } },
                        { is_async = true }
                    )
                    if not rpc_send_ok_ok then
                        log.error(rpc_send_ok_err)
                        return nil, rpc_send_ok_err
                    end
                end
                --TODO Error check
            end

            if fun.length(valid_messages) > 0 then
                log.info("Sending success msg to kafka")
                for k, v in pairs(fun.tomap(valid_messages)) do
                    metrics.kafka_messages_total_counter:inc(1, { status = "success" })
                    -- luacheck: ignore send_messages_to_kafka
                    local rpc_send_ok_ok, rpc_send_ok_err = send_messages_to_kafka(
                        route_utils.get_topic_success()[v["topic"]],
                        {
                            [1] = {
                                key = v["key"],
                                value = success_repository.get_success_code("ADG_KAFKA_CONNECTOR_001", {
                                    source = v["topic"],
                                    destination = v["space"],
                                }),
                            },
                        },
                        { is_async = true }
                    )
                    if not rpc_send_ok_ok then
                        log.error(rpc_send_ok_err)
                        return nil, rpc_send_ok_err
                    end
                end
                --TODO Error check
            end

            log.warn("INFO: Commiting to kafka")
            local commit_ok, commit_err = kafka_consumer.commit(last_messages)
            if commit_ok == true then
                log.warn("INFO: Commit succeseful")
            else
                return nil,
                    error_repository.get_error_code("ADG_KAFKA_CONNECTOR_001", {
                        messages = last_messages,
                        error = commit_err,
                    }) --TODO Agg with consumer error
            end
            fiber.sleep(0.2)
        else
            log.info("INFO: No new messages from kafka")
            fiber.sleep(0.2)
        end
        fiber.sleep(0.2)
    end
end

-- luacheck: ignore opts
local function send_messages_to_kafka(topic_name, messages, opts)
    local is_send, err = producer:produce_async(topic_name, messages)
    return is_send, err
end

local function get_metric()
    return metrics.export(role_name)
end

local function subscribe_to_topic_fiber(
    topic_name,
    spaces,
    avro_schema,
    max_number_of_messages_per_cb,
    max_number_of_seconds_per_cb,
    cb_function_name,
    cb_function_param
)
    checks("string", "table", "?string", "?number", "?number", "?string", "?table")

    if (max_number_of_messages_per_cb ~= nil or max_number_of_seconds_per_cb ~= nil) and cb_function_name == nil then
        return false, "ERROR: Please specify cb_function_name param"
    end

    max_number_of_messages_per_cb = max_number_of_messages_per_cb or 1000 -- TODO default to config
    max_number_of_seconds_per_cb = max_number_of_seconds_per_cb or 600 -- TODO default to config

    local kafka_topics = box.space["_KAFKA_TOPIC"]

    if kafka_topics == nil then
        return false, error_repository.get_error_code("STORAGE_001", { table = "_KAFKA_TOPIC" })
    end

    box.begin()
    local res, err = err_storage:pcall(function()
        local cnt = kafka_topics:count(topic_name)

        kafka_topics:put(
            kafka_topics:frommap({
                TOPIC_NAME = topic_name,
                MAX_NUMBER_OF_MESSAGES_PER_PARTITION_WITH_CB_CALL = max_number_of_messages_per_cb,
                SPACE_NAMES = spaces,
                AVRO_SCHEMA = avro_schema,
                CALLBACK_FUNCTION_NAME = cb_function_name,
                CALLBACK_FUNCTION_PARAMS = cb_function_param,
                MAX_IDLE_SECONDS_BEFORE_CB_CALL = max_number_of_seconds_per_cb,
            })
        )
        if cnt == 1 and topic_x_consumers[topic_name] ~= nil then
            return false
        end

        return true
    end)

    if res == false then
        box.rollback()
        return true, nil
    end

    if err ~= nil then
        box.rollback()
        return false, error_repository.get_error_code("STORAGE_003", { error = err })
    end

    box.commit()

    local is_subscribed, subscribe_err = default_consumer:subscribe({ topic_name })

    if not is_subscribed then
        return false, subscribe_err
    end

    return true, nil
end

local function unsubscribe_from_topic_fiber(topic_name)
    checks("string")
    local kafka_topics = box.space["_KAFKA_TOPIC"]
    local kafka_topics_prev = box.space["_KAFKA_TOPIC_PREV"]
    if kafka_topics == nil then
        return false, error_repository.get_error_code("STORAGE_001", { table = "_KAFKA_TOPIC" })
    end

    if kafka_topics_prev == nil then
        return false, error_repository.get_error_code("STORAGE_001", { table = "_KAFKA_TOPIC_PREV" })
    end

    box.begin()
    local topic_found, err = err_storage:pcall(function()
        local cnt = kafka_topics:count(topic_name)
        if cnt == 1 then
            kafka_topics_prev:put(kafka_topics:get(topic_name))
            kafka_topics:delete(topic_name)
            return true
        end
        return true
    end)

    if err ~= nil then
        box.rollback()
        return false, error_repository.get_error_code("STORAGE_003", { error = err })
    end

    box.commit()

    if topic_found then
        local is_unsubscribe, unsubscribe_err = default_consumer:unsubscribe({ topic_name })

        if not is_unsubscribe then
            return false, unsubscribe_err
        end

        return true, nil
    end

    return false, nil -- topic not found for 404.
end

local function dataload_from_topic_fiber(topic_name, spaces, max_number_of_messages, avro_schema)
    checks("string", "table", "?number", "?string")
    local kafka_topics = box.space["_KAFKA_TOPIC"]

    if kafka_topics == nil then
        return { false, error = error_repository.get_error_code("STORAGE_001", { table = "_KAFKA_TOPIC" }), amount = 0 }
    end

    local init_data = kafka_topics:get(topic_name)

    if init_data == nil then
        return { false, error = "ERROR: Please subscribe first", amount = 0 }
    end

    avro_schema = avro_schema or init_data.AVRO_SCHEMA
    max_number_of_messages = max_number_of_messages or init_data.MAX_NUMBER_OF_MESSAGES_PER_PARTITION
    local consumer = topic_x_consumers[topic_name]

    local msg_polled, msgs = consumer:poll_messages(max_number_of_messages)

    if not msg_polled then
        return { false, error = msgs, amount = 0 }
    end
    if msgs.amount > 0 then
        local serialized_kafka_msgs = fun.map(function(v)
            return v:value()
        end, msgs.result):totable()
        local msg_with_max_offset = fun.max_by(function(a, b)
            if a:offset() > b:offset() then
                return a
            else
                return b
            end
        end, msgs.result)
        local msg_process_status, msg_process_err = cartridge.rpc_call(
            "app.roles.adg_input_processor",
            "insert_messages_from_kafka",
            { serialized_kafka_msgs, "parse_binary_avro", "parse_binary_avro", spaces, avro_schema }
        )

        if msg_process_status == nil then
            return { false, error = msg_process_err or "ERROR: insert_messages_from_kafka failed", amount = 0 }
        end

        if not msg_process_status[1] then
            return msg_process_status[2] --suggest {true|false, nil|{error,amount}}
        end

        local is_msgs_committed, msgs_commit_err = consumer:commit(msg_with_max_offset)

        if not is_msgs_committed then
            return { false, error = msgs_commit_err, amount = msg_process_status[2].amount }
        end

        return { true, amount = msgs.amount }
    else
        return { true, amount = 0 }
    end
end

local function subscribe_to_topic(
    topic_name,
    spaces,
    avro_schema,
    max_number_of_messages_per_cb,
    max_number_of_seconds_per_cb,
    cb_function_name,
    cb_function_param
)
    checks("string", "table", "?string", "?number", "?number", "?string", "?table")
    local f = fiber.new(
        subscribe_to_topic_fiber,
        topic_name,
        spaces,
        avro_schema,
        max_number_of_messages_per_cb,
        max_number_of_seconds_per_cb,
        cb_function_name,
        cb_function_param
    )
    f:name("s_" .. topic_name:sub(1, 10) .. "_" .. table.concat(spaces, ","):sub(1, 10))
    f:set_joinable(true)
    local is_fiber_ok, res = f:join()
    if is_fiber_ok then
        return res
    else
        return is_fiber_ok, res
    end
end

local function unsubscribe_from_topic(topic_name)
    checks("string")
    local f = fiber.new(unsubscribe_from_topic_fiber, topic_name)
    f:name("uns_" .. topic_name:sub(1, 10))
    f:set_joinable(true)
    local is_fiber_ok, res = f:join()
    if is_fiber_ok then
        return res
    else
        return is_fiber_ok, res
    end
end

local function dataload_from_topic(topic_name, spaces, max_number_of_messages, avro_schema)
    checks("string", "table", "?number", "?string")
    local f = fiber.new(dataload_from_topic_fiber, topic_name, spaces, max_number_of_messages, avro_schema)
    f:name("dload_" .. topic_name:sub(1, 10))
    f:set_joinable(true)
    local is_fiber_ok, res = f:join()
    if is_fiber_ok then
        return res[1], res.error, res.amount
    else
        return is_fiber_ok, res
    end
end

local function subscribe_to_all_topics()
    local kafka_topics = box.space["_KAFKA_TOPIC"]
    if kafka_topics == nil then
        return false, error_repository.get_error_code("STORAGE_001", { table = "_KAFKA_TOPIC" })
    end

    for _, v in kafka_topics.index.TOPIC_NAME:pairs() do
        local res, err = subscribe_to_topic(
            v.TOPIC_NAME,
            v.SPACE_NAMES,
            v.AVRO_SCHEMA,
            v.MAX_NUMBER_OF_MESSAGES_PER_PARTITION_WITH_CB_CALL,
            v.MAX_IDLE_SECONDS_BEFORE_CB_CALL,
            v.CALLBACK_FUNCTION_NAME,
            v.CALLBACK_FUNCTION_PARAMS
        )
        if not res then
            return false, err
        end
    end

    return true, nil
end

local function init_metatables()
    local kafka_topics = box.schema.space.create("_KAFKA_TOPIC", {
        format = {
            { "TOPIC_NAME", "string" },
            { "MAX_NUMBER_OF_MESSAGES_PER_PARTITION_WITH_CB_CALL", "unsigned" },
            { "SPACE_NAMES", "array" },
            { "AVRO_SCHEMA", "string", is_nullable = true },
            { "CALLBACK_FUNCTION_NAME", "string", is_nullable = true },
            { "CALLBACK_FUNCTION_PARAMS", "map", is_nullable = true },
            { "MAX_IDLE_SECONDS_BEFORE_CB_CALL", "unsigned", is_nullable = true },
        },
        if_not_exists = true,
    })

    kafka_topics:create_index("TOPIC_NAME", {
        parts = { "TOPIC_NAME" },
        type = "HASH",
        if_not_exists = true,
    })

    local kafka_topics_prev = box.schema.space.create("_KAFKA_TOPIC_PREV", {
        format = {
            { "TOPIC_NAME", "string" },
            { "MAX_NUMBER_OF_MESSAGES_PER_PARTITION_WITH_CB_CALL", "unsigned" },
            { "SPACE_NAMES", "array" },
            { "AVRO_SCHEMA", "string", is_nullable = true },
            { "CALLBACK_FUNCTION_NAME", "string", is_nullable = true },
            { "CALLBACK_FUNCTION_PARAMS", "map", is_nullable = true },
            { "MAX_IDLE_SECONDS_BEFORE_CB_CALL", "unsigned", is_nullable = true },
        },
        if_not_exists = true,
    })

    kafka_topics_prev:create_index("TOPIC_NAME_P", {
        parts = { "TOPIC_NAME" },
        type = "HASH",
        if_not_exists = true,
    })

    local kafka_msgs = box.schema.space.create("_KAFKA_TOPIC_PARTITION_STAT", {
        format = {
            { "TOPIC_NAME", "string" },
            { "PARTITION_NAME", "string" },
            { "MSG_CNT", "unsigned" },
            { "LAST_MSG_TIMESTAMP", "number" },
        },
        if_not_exists = true,
    })

    kafka_msgs:create_index("IX_TOPIC_PARTITION", {
        parts = { "TOPIC_NAME", "PARTITION_NAME" },
        type = "HASH",
        if_not_exists = true,
    })
end

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates("app.roles.adg_storage", { leader_only = true })) do
        return cartridge_rpc.call("app.roles.adg_storage", "get_schema", nil, { uri = instance_uri })
    end
end

local function init(opts)
    rawset(_G, "ddl", { get_schema = get_schema })

    _G.get_messages_from_kafka = get_messages_from_kafka
    _G.send_messages_to_kafka = send_messages_to_kafka
    _G.subscribe_to_topic = subscribe_to_topic
    _G.unsubscribe_from_topic = unsubscribe_from_topic
    _G.dataload_from_topic = dataload_from_topic

    kafka_utils.init_kafka_opts()

    garbage_fiber = fiber.create(function()
        while true do
            collectgarbage("step", 20)
            fiber.sleep(0.2)
        end
    end)

    garbage_fiber:name("GARBAGE_COLLECTOR_FIBER")

    local is_producer_ok -- luacheck: ignore is_producer_ok
    is_producer_ok, producer = kafka_producer.init(kafka_utils.get_brokers()):build()

    local is_default_consumer_ok -- luacheck: ignore is_default_consumer_ok
    is_default_consumer_ok, default_consumer = kafka_consumer.init(kafka_utils.get_brokers())
        :set_options(kafka_utils.get_options())
        :build()

    default_consumer:init_poll_msg_fiber()

    if opts.is_master then
        init_metatables()
    end

    _G.get_metric = get_metric

    local _, _ = subscribe_to_all_topics()

    local httpd = cartridge.service_get("httpd")
    httpd:route({ method = "GET", path = "/metrics" }, prometheus.collect_http)

    return true
end

return {
    role_name = role_name,
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    get_messages_from_kafka = get_messages_from_kafka,
    send_messages_to_kafka = send_messages_to_kafka,
    get_metric = get_metric,
    get_schema = get_schema,
    subscribe_to_topic = subscribe_to_topic,
    unsubscribe_from_topic = unsubscribe_from_topic,
    dataload_from_topic = dataload_from_topic,
    dependencies = {
        "cartridge.roles.crud-router",
        "cartridge.roles.vshard-router",
    },
}
