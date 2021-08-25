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

local checks = require("checks")
local errors = require("errors")

local route_utils = require("app.utils.route_utils")
local inserted_tuples = {}
local track_inserted_tuples = false

local err_sql_insert = errors.new_class("SQL insert error")

-- luacheck: ignore old_value request_type
local function on_replace_trigger(old_value, new_value, space_name, request_type)
    if track_inserted_tuples then
        local tuples = inserted_tuples[space_name]

        if tuples == nil then
            tuples = {}
            inserted_tuples[space_name] = tuples
        end

        table.insert(tuples, new_value)
    end
end

local function install_triggers()
    for key, space in pairs(box.space) do
        if not string.startswith(space.name, "_") and type(key) == "string" then
            pcall(space.on_replace, space, nil, on_replace_trigger)
            space:on_replace(on_replace_trigger)
        end
    end
end

-- Get tuples that will be inserted by an sql statement
local function get_tuples(sql_statement, params, bucket_count)
    checks("string", "table", "number")

    inserted_tuples = {}
    track_inserted_tuples = true

    --TODO refactor this shit
    local res, err = err_sql_insert:pcall(function()
        box.begin()
        -- luacheck: ignore sql_result
        local sql_result = nil
        -- luacheck: ignore err
        local err = nil
        if params == nil then
            sql_result, err = err_sql_insert:pcall(box.execute, sql_statement)
        else
            sql_result, err = err_sql_insert:pcall(box.execute, sql_statement, params)
        end
        box.rollback()

        if err ~= nil then
            return nil, err
        end

        return { sql_result = sql_result, inserted_tuples = inserted_tuples }
    end)

    inserted_tuples = {}
    track_inserted_tuples = false

    if res == nil then
        return nil, err
    end

    for space, tuples in pairs(res.inserted_tuples) do
        for i, tuple in ipairs(tuples) do
            tuples[i] = route_utils.set_bucket_id(space, tuple, bucket_count)
        end
    end

    return res, err
end

-- luacheck: ignore default
local function get_default(tbl, key, default)
    local res = tbl[key]

    if res ~= nil then
        return res
    end

    res = {}
    tbl[key] = res
    return res
end

local function tuples_by_bucket_id(tuples, bucket_count)
    local result = {}

    for space_name, per_space in pairs(tuples) do
        for _, tuple in ipairs(per_space) do
            local bucket_id = route_utils.get_bucket_id(space_name, tuple, bucket_count)

            local per_bucket_id = get_default(result, bucket_id, {})
            local per_space_name = get_default(per_bucket_id, space_name, {})
            table.insert(per_space_name, tuple)
        end
    end

    return result
end

return {
    install_triggers = install_triggers,
    get_tuples = get_tuples,
    tuples_by_bucket_id = tuples_by_bucket_id,
}
