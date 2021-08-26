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

local cartridge = require("cartridge")
local prometheus = require("metrics.plugins.prometheus")
local errors = require("errors")
local vshard = require("vshard")
local log = require("log")
local checks = require("checks")
local schema_utils = require("app.utils.schema_utils")
local route_utils = require("app.utils.route_utils")
local avro_utils = require("app.utils.avro_utils")
local bin_avro_utils = require("app.utils.bin_avro_utils")
local avro_schema_utils = require("app.utils.avro_schema_utils")
local fun = require("fun")
local json = require("json")
local error_repository = require("app.messages.error_repository")
local success_repository = require("app.messages.success_repository")
local garbage_fiber = nil
local cache_clear_fiber = nil
local fiber = require("fiber")
local validate_utils = require("app.utils.validate_utils")
local yaml = require("yaml")

local cartridge_rpc = require("cartridge.rpc")

local role_name = "app.roles.adg_input_processor"

_G.insert_messages_from_kafka = nil
_G.load_csv_lines = nil
_G.get_metric = nil
_G.insert_message_from_kafka_async = nil

local metrics = require("app.metrics.metrics_storage")

local schema_cache = {}

local function stop()
    garbage_fiber:cancel()
    cache_clear_fiber:cancel()
    return true
end

-- luacheck: ignore conf_old
local function validate_config(conf_new, conf_old)
    if type(box.cfg) ~= "function" and not box.cfg.read_only then
        local kafka_topics = yaml.decode(conf_new["kafka_topics.yml"] or [[]]) or {}
        local kafka_consumers = yaml.decode(conf_new["kafka_consume.yml"] or [[]])
            or { ["topics"] = {}, ["properties"] = {}, ["custom_properties"] = {} }

        local is_topic_defs_ok, topic_defs_err = validate_utils.check_topic_definition(
            kafka_consumers["topics"],
            kafka_topics
        )

        if not is_topic_defs_ok then
            return false, topic_defs_err
        end

        local schema_registry_opts = yaml.decode(conf_new["kafka_schema_registry.yml"] or [[]])
            or { ["host"] = "localhost", ["port"] = 8081 }

        local is_schema_registry_opts_ok, schema_registry_opts_err = validate_utils.check_schema_registry_opts(
            schema_registry_opts
        )

        if not is_schema_registry_opts_ok then
            return false, schema_registry_opts_err
        end
    end
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    if opts.is_master and pcall(vshard.storage.info) == false then
        schema_utils.drop_all()
    end
    schema_utils.init_schema_ddl()
    route_utils.init_routes()
    avro_schema_utils.init_routes()
    error_repository.init_error_repo("en")
    success_repository.init_success_repo("en")
    return true
end

local function load_csv_lines(space_name, lines)
    checks("string", "table")
    local space = schema_utils.get_schema_ddl().spaces[space_name]
        or schema_utils.get_schema_ddl().spaces[string.upper(space_name)]

    if space == nil then
        return nil, errors.new("ERROR: no_such_space", "No such space: %s", space_name)
    end

    local tuples = {}

    for _, line in ipairs(lines) do
        local err
        line, err = schema_utils.from_csv_line(space_name, line)

        if line == nil then
            return nil, err
        end

        local tuple, err_bucket = route_utils.set_bucket_id(space_name, line, vshard.router.bucket_count())

        if tuple == nil then
            return nil, err_bucket
        end
        table.insert(tuples, tuple)
    end

    local futures = {}

    for server, per_server in pairs(route_utils.tuples_by_server(tuples, space_name, vshard.router.bucket_count())) do
        local future = server:call("insert_tuples", { { [space_name] = per_server } }, { is_async = true })
        table.insert(futures, future)
    end

    for _, future in ipairs(futures) do
        future:wait_result()
        local res, err = future:result()
        if res == nil then
            return nil, err
        end
    end

    return true
end

-- luacheck: ignore topic value
local function parse_csv(topic, value)
    -- TODO multiple string parse
end

local function parse_avro(schema, value)
    checks("string", "string")
    --check value for json or bin?
    local parsed_schema, key = avro_schema_utils.get_schema(schema) -- Performance????

    if parsed_schema == nil then
        return nil,
            error_repository.get_error_code("AVRO_SCHEMA_001", {
                schema_registry = avro_schema_utils.get_schema_registry_opts(),
                schema_name = schema,
            })
    end

    local is_valid_json, json_value = pcall(json.decode, value)

    if not is_valid_json then
        return nil, error_repository.get_error_code("AVRO_SCHEMA_004", {
            desc = json_value,
        })
    end

    -- Performance?

    local is_valid, normalized_data = avro_utils.validate_avro_data(parsed_schema, json_value)
    if not is_valid then
        return false,
            error_repository.get_error_code("AVRO_SCHEMA_005", {
                error = normalized_data,
            })
    end

    log.info("INFO: Avro data validated against schema")

    local methods = schema_cache[key] or nil

    if methods == nil then
        local is_compile, methods = avro_utils.compile_avro_schema(parsed_schema) -- Performance????
        if not is_compile then
            return nil,
                error_repository.get_error_code("AVRO_SCHEMA_002", {
                    schema_name = parsed_schema,
                    methods = methods,
                })
        end
        log.info("INFO: Avro schema successfully compiled")
        schema_cache[key] = methods
    end

    local is_generate, data = schema_cache[key].flatten(normalized_data) --MSG Pack????
    if not is_generate then
        return false, error_repository.get_error_code("AVRO_SCHEMA_006", { error = data })
    end
    return true, data
end

local function parse_binary_avro(value)
    checks({
        value = "string",
        opts = {
            avro_schema = "?string",
        },
    })

    local is_value_decode, decode_value = bin_avro_utils.decode(value.value, value.opts.avro_schema)

    if not is_value_decode then
        return false, decode_value
    end

    return true, decode_value
end

local function get_function_by_name(function_name)
    checks("string")
    if function_name == "parse_csv" then
        return parse_csv
    end
    if function_name == "parse_avro" then
        return parse_avro
    end
    if function_name == "parse_binary_avro" then
        return parse_binary_avro
    end
    error_repository.get_error_code("ADG_INPUT_PROCESSOR_001", { function_name = function_name })
    return nil
end

local function decode_value_w_function(value, parse_function_str)
    checks({
        value = "string",
        opts = "?table",
    }, "string")
    local parse_function = get_function_by_name(parse_function_str)
    if parse_function == nil then
        return false, string.format("ERROR: function %s not found", parse_function_str)
    end
    return parse_function(value)
end

local function load_value_to_storage(value, spaces)
    checks("table", "table")
    local result = {}
    for _, space in ipairs(spaces) do
        --check if space exists
        local space_check = schema_utils.get_schema_ddl().spaces[space]
            or schema_utils.get_schema_ddl().spaces[string.upper(space)]

        if space_check == nil then
            result[space] = {
                result = false,
                desc = {
                    error = string.format("ERROR: No such space: %s", space),
                    amount = 0,
                },
            }
            goto continue
        end

        local tuples = {}
        for _, row in ipairs(value) do
            local tuple, err_bucket = route_utils.set_bucket_id(space, row, vshard.router.bucket_count(), false)
            if tuple == nil then
                result[space] = { result = false, desc = { error = err_bucket, amount = 0 } }
                goto continue
            end
            table.insert(tuples, tuple)
        end
        local futures = {}

        for server, per_server in pairs(route_utils.tuples_by_server(tuples, space, vshard.router.bucket_count())) do
            local future = server:call("insert_tuples", { { [space] = per_server }, false }, { is_async = true })
            table.insert(futures, future)
        end

        local rows_inserted = 0
        for _, future in ipairs(futures) do
            future:wait_result(300)
            local res, err = future:result()

            if res == nil then
                result[space] = { result = false, desc = { error = err, amount = 0 } }
                goto continue
            end

            if res[1] == nil then
                result[space] = { result = false, desc = { error = res[2].err or res[2], amount = 0 } }
                goto continue
            end

            result[space] = {
                result = false,
                desc = {
                    error = string.format("ERROR: function %s error", "insert_tuples"),
                    amount = res[1],
                },
            }
            rows_inserted = rows_inserted + res[1]
        end
        result[space] = { result = true, desc = { error = nil, amount = rows_inserted } }

        ::continue::
    end

    local is_all_loaded = fun.all(function(_, v)
        return v.result
    end, result)

    return is_all_loaded, result
end

-- luacheck: ignore parse_key_function_str
local function insert_messages_from_kafka(
    messages,
    parse_key_function_str,
    parse_value_function_str,
    spaces,
    avro_schema
)
    checks("table", "string", "string", "table", "?string")
    local loaded_msg = 0
    for _, v in ipairs(messages) do
        --extract msg info
        local value = v

        local is_value_decoded, decoded_value = decode_value_w_function(
            { value = value, opts = { avro_schema = avro_schema } },
            parse_value_function_str
        )

        if not is_value_decoded then
            return { false, { error = decoded_value, amount = 0 } }
        end

        --check row or table of rows?
        if type(decoded_value) ~= "table" then
            decoded_value = { { decoded_value } }
        end

        if type(decoded_value[1]) ~= "table" then
            decoded_value = { decoded_value }
        end

        local is_value_loaded, loaded_value = load_value_to_storage(decoded_value, spaces)

        if not is_value_loaded then
            -- luacheck: ignore k
            local loaded_rows_cnt = fun.map(function(k, v)
                return v.desc.amount
            end, loaded_value):sum() --loaded rows
            local concatenate_error = fun.filter(function(k, v)
                return not v.result
            end, loaded_value) --filter errors
                :map(function(k, v)
                    return k .. ": " .. tostring(v.desc.error)
                end) --extract error
                :foldl(function(acc, error)
                    return acc .. error .. ";"
                end, "") -- concatenate
            return { false, { error = concatenate_error, amount = loaded_msg, loaded_rows_cnt = loaded_rows_cnt } }
        end

        loaded_msg = loaded_msg + 1
    end

    return { true, { amount = loaded_msg } }
end

local function insert_message_from_kafka_async(
    message,
    parse_key_function_str,
    parse_value_function_str,
    spaces,
    avro_schema
)
    checks("?string", "string", "string", "table", "?string")

    if message == nil then
        return { true, { amount = 0 } }
    end

    --extract msg info
    local value = message

    local is_value_decoded, decoded_value = decode_value_w_function(
        { value = value, opts = { avro_schema = avro_schema } },
        parse_value_function_str
    )

    if not is_value_decoded then
        return { false, { error = decoded_value, amount = 0 } }
    end

    --check row or table of rows?
    if type(decoded_value) ~= "table" then
        decoded_value = { { decoded_value } }
    end

    if type(decoded_value[1]) ~= "table" then
        decoded_value = { decoded_value }
    end

    local is_value_loaded, loaded_value = load_value_to_storage(decoded_value, spaces)

    if not is_value_loaded then
        -- luacheck: ignore k
        local loaded_rows_cnt = fun.map(function(k, v)
            return v.desc.amount
        end, loaded_value):sum() --loaded rows
        local concatenate_error = fun.filter(function(k, v)
            return not v.result
        end, loaded_value) --filter errors
            :map(function(k, v)
                return k .. ": " .. tostring(v.desc.error)
            end) --extract error
            :foldl(function(acc, error)
                return acc .. error .. ";"
            end, "") -- concatenate
        return { false, { error = concatenate_error, amount = 1, loaded_rows_cnt = loaded_rows_cnt } }
    end

    return { true, { amount = 1 } }
end

local function get_metric()
    return metrics.export(role_name)
end

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates("app.roles.adg_storage", { leader_only = true })) do
        return cartridge_rpc.call("app.roles.adg_storage", "get_schema", nil, { uri = instance_uri })
    end
end

local function init(opts)
    rawset(_G, "ddl", { get_schema = get_schema })

    _G.insert_messages_from_kafka = insert_messages_from_kafka
    _G.load_csv_lines = load_csv_lines
    _G.insert_message_from_kafka_async = insert_message_from_kafka_async
    if opts.is_master then -- luacheck: ignore 542
    end

    garbage_fiber = fiber.create(function()
        while true do
            collectgarbage("step", 20)
            fiber.sleep(0.2)
        end
    end)
    garbage_fiber:name("GARBAGE_COLLECTOR_FIBER")

    cache_clear_fiber = fiber.create(function()
        while true do
            schema_cache = {}
            fiber.sleep(600)
        end
    end)
    cache_clear_fiber:name("CLEAR_CACHE_FIBER")

    _G.get_metric = get_metric

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
    insert_messages_from_kafka = insert_messages_from_kafka,
    insert_message_from_kafka_async = insert_message_from_kafka_async,
    get_schema = get_schema,
    load_csv_lines = load_csv_lines,
    get_metric = get_metric,
    dependencies = {
        "cartridge.roles.crud-router",
        "cartridge.roles.vshard-router",
    },
}
