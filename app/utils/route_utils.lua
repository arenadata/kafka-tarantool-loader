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

local digest = require("digest")
local schema_utils = require("app.utils.schema_utils")
local config_utils = require("app.utils.config_utils")
local routes = {}
local topic_error = {}
local topic_success = {}
local vshard = require("vshard")
local checks = require("checks")
local log = require("log")
local error_repository = require("app.messages.error_repository")

local function init_routes()
    routes = config_utils.get_topics_x_targets(config_utils.get_config())
    topic_error = config_utils.get_topics_x_error(config_utils.get_config())
    topic_success = config_utils.get_topics_x_success(config_utils.get_config())
end

local function get_routes()
    return routes
end

local function get_topic_error()
    return topic_error
end

local function get_topic_success()
    return topic_success
end

local function get_bucket_id(space_name, tuple, bucket_count, is_record_type)
    checks("string", "cdata|table", "number", "?boolean")
    is_record_type = is_record_type or false

    local c = digest.murmur.new()

    local fields = schema_utils.get_bucket_id_fields(space_name, is_record_type)

    for _, key in ipairs(fields) do
        c:update(tostring(tuple[key]))
    end

    return digest.guava(c:result(), bucket_count - 1) + 1
end

local function set_bucket_id(space_name, tuple, bucket_count, is_record_type)
    is_record_type = is_record_type or false
    local bucket_id_field = schema_utils.get_field_by_name(space_name, "bucket_id")

    if bucket_id_field == nil then
        return tuple
    end

    local bucket_id = get_bucket_id(space_name, tuple, bucket_count, is_record_type)

    if type(tuple) == "cdata" then
        return tuple:update({ { "=", bucket_id_field, bucket_id } })
    else
        if is_record_type then
            tuple["bucket_id"] = bucket_id
        else
            tuple[bucket_id_field] = bucket_id
        end
        return tuple
    end
end

local function get_default(tbl, key)
    local res = tbl[key]

    if res ~= nil then
        return res
    end

    res = {}
    tbl[key] = res
    return res
end

local function tuples_by_server(tuples, space_name, bucket_count)
    local result = {}

    for _, tuple in ipairs(tuples) do
        local bucket_id = get_bucket_id(space_name, tuple, bucket_count)
        local replica, err = vshard.router.route(bucket_id)

        if err ~= nil then
            log.error(err)
        end

        local per_server = get_default(result, replica, {})
        table.insert(per_server, tuple)
    end

    return result
end

local function get_space_by_topic(topic_name)
    if routes[topic_name] == nil then
        return nil, error_repository.get_error_code("AVRO_SCHEMA_007", { topic_name = topic_name })
    end
    local space = routes[topic_name]
    if space == nil then
        return nil, error_repository.get_error_code("STORAGE_001", { space = space })
    else
        return routes[topic_name], nil
    end
end

return {
    init_routes = init_routes,
    get_bucket_id = get_bucket_id,
    set_bucket_id = set_bucket_id,
    get_default = get_default,
    tuples_by_server = tuples_by_server,
    get_routes = get_routes,
    get_space_by_topic = get_space_by_topic,
    get_topic_error = get_topic_error,
    get_topic_success = get_topic_success,
}
