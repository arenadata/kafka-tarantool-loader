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

local config_utils = require("app.utils.config_utils")
local ddl = require("ddl")
local errors = require("errors")
local checks = require("checks")

local cartridge = require("cartridge")

local schema_ddl = {}

local err_ddl = errors.new_class("Schema_utils error")
local bucket_id_fields_cache = {}

local function clean_bucket_id_cache_by_space(space_name)
    bucket_id_fields_cache[space_name] = nil
end

local function init_schema_ddl()
    schema_ddl = config_utils.get_ddl_schema(config_utils.get_config())
end

local function get_schema_ddl()
    schema_ddl = cartridge.config_get_deepcopy().schema
    return schema_ddl
end

local function check_schema(s)
    checks("table")

    if s == "" then
        return true
    end

    local res, err = ddl.check_schema(s)

    return res, err
end

local function set_schema(s)
    if s == nil then
        schema_ddl = {}
        return true
    end

    local res, err = ddl.set_schema(s)

    if res == nil then
        return nil, err
    end

    schema_ddl = s

    return res
end

local function get_current_schema_ddl()
    return ddl.get_schema()
end

local function get_field_by_name(space, field_name) -- called on the api role and need to apply the cluster schema
    for id, field in ipairs(get_schema_ddl().spaces[space].format) do
        if field.name == field_name then
            return id
        end
    end

    return nil
end

local function get_bucket_id_fields(space_name, is_record_type) -- called on the api role and need to apply the cluster schema
    local cache = bucket_id_fields_cache[space_name]

    if cache ~= nil then
        return cache
    end

    cache = {}
    for _, key in ipairs(get_schema_ddl().spaces[space_name].sharding_key) do
        if not is_record_type then
            table.insert(cache, get_field_by_name(space_name, key))
        else
            table.insert(cache, key)
        end
    end

    bucket_id_fields_cache[space_name] = cache

    return cache
end

local function drop_all()
    return err_ddl:pcall(function()
        for key, space in pairs(box.space) do
            if not string.startswith(space.name, "_") and type(key) == "string" then
                space:drop()
            end
        end

        return true
    end)
end

local function from_csv_line(space_name, line)
    checks("string", "table")
    local res = {}

    for id, field in ipairs(schema_ddl.spaces[space_name].format) do
        --log.error('!!' .. space_name)
        if field.type == "number" then
            res[id] = tonumber(line[id])
            if res[id] == nil then
                res[id] = box.NULL
            end

            if res[id] == nil and field.is_nullable == false then
                return nil, errors.new("field_cant_be_nil", "Field %s.%s is not nullable", space_name, field.name)
            end
        else
            res[id] = line[id]
        end
    end
    return res
end

return {
    clean_bucket_id_cache_by_space = clean_bucket_id_cache_by_space,
    init_schema_ddl = init_schema_ddl,
    get_schema_ddl = get_schema_ddl,
    get_field_by_name = get_field_by_name,
    get_bucket_id_fields = get_bucket_id_fields,
    drop_all = drop_all,
    from_csv_line = from_csv_line,
    check_schema = check_schema,
    set_schema = set_schema,
    get_current_schema_ddl = get_current_schema_ddl,
}
