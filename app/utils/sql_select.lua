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

local sql = require("app.utils.sql")
local errors = require("errors")
local vshard = require("vshard")

local fun = require("fun")
local digest = require("digest")
local checks = require("checks")

local schema_utils = require("app.utils.schema_utils")

local VALUE = 1
local PARAM = 2

local query_cache = {}

local function prepare(query)
    checks("string")

    local parsed = sql.parse(query)

    if parsed == nil or parsed[1] ~= "select" then
        return nil
    end

    local expr = parsed[4]

    local space_name = parsed[3]

    if type(space_name) ~= "string" then
        return nil, errors.new("calculate_bucket_id_error", "only selects from single table are supported")
    end

    local schema = schema_utils.get_schema_ddl()

    if schema == nil or schema.spaces == nil then
        return nil, errors.new("calculate_bucket_id_error", "schema not set")
    end

    local space = schema.spaces[space_name] or schema.spaces[string.upper(space_name)]

    if space == nil then
        return nil, errors.new("calculate_bucket_id_error", "No such space in schema: %s", space_name)
    end

    local sharding_key = space.sharding_key

    if sharding_key == nil then
        return nil, errors.new("calculate_bucket_id_error", "Sharding key not set for space: %s", space)
    end

    local conditions = {}

    if expr == nil then
        return {
            space_name = space_name,
            sharding_key = sharding_key,
            conditions = conditions,
        }
    end

    if expr[1] == "and" then
        for _, subexpr in fun.tail(expr) do
            if type(subexpr) == "table" then
                if subexpr[1] == "=" then
                    local value = subexpr[3]

                    if type(value) == "table" and value[1] == "param" then
                        conditions[subexpr[2]] = { PARAM, value[2] }
                    elseif type(value) == "table" then
                        conditions[subexpr[2]] = nil
                    else
                        conditions[subexpr[2]] = { VALUE, value }
                    end
                end
            end
        end
    elseif expr[1] == "=" then
        local value = expr[3]

        if type(value) == "table" and value[1] == "param" then
            conditions[expr[2]] = { PARAM, value[2] }
        else
            conditions[expr[2]] = { VALUE, value }
        end
    end

    return {
        space_name = space_name,
        sharding_key = sharding_key,
        conditions = conditions,
    }
end

local function calculate_bucket_id(query, params, bucket_count)
    checks("string", "table", "number")
    params = params or {}

    local prepared = query_cache[query]

    if prepared == nil then
        local err
        prepared, err = prepare(query)

        if prepared == nil then
            return nil, err
        end
        query_cache[query] = prepared
    end

    local conditions = prepared.conditions

    local c = digest.murmur.new()
    for _, element in ipairs(prepared.sharding_key) do
        local condition = conditions[element]

        if condition == nil then
            return nil
        end

        local value = nil
        if condition[1] == VALUE then
            value = condition[2]
        elseif condition[1] == PARAM then
            value = params[condition[2]]
        end

        c:update(tostring(value))
    end

    return digest.guava(c:result(), bucket_count - 1) + 1
end

local function get_replicas(query, params)
    local all_replicas, err = vshard.router.routeall()
    params = params or {}

    if all_replicas == nil then
        return nil, err
    end

    local bucket_id, err = calculate_bucket_id(query, params, vshard.router.bucket_count())

    if err ~= nil then
        return nil, err
    elseif bucket_id == nil then
        return all_replicas
    end

    local replica, err = vshard.router.route(bucket_id)

    if replica == nil then
        return nil, err
    end

    return { replica }
end

local function clear_cache()
    query_cache = {}
end

-- luacheck: ignore query_result
local function get_bucket_id_column_number(query_result)
    local result = nil

    -- luacheck: ignore res
    for k, v in ipairs(res[1]["metadata"]) do
        if v["name"] == "bucket_id" then
            result = k
        end
    end

    return result
end

return {
    get_replicas = get_replicas,
    calculate_bucket_id = calculate_bucket_id,
    clear_cache = clear_cache,
    get_bucket_id_column_number = get_bucket_id_column_number,
}
