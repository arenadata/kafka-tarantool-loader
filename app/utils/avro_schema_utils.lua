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

local log = require('log')
local avro_schema = require('avro_schema')
local json = require('json')
local config_utils = require('app.utils.config_utils')
-- local misc_utils = require('app.utils.misc_utils')
-- local avro_utils = require('app.utils.avro_utils')
local checks = require('checks')
local fun = require('fun')
local error_repository = require('app.messages.error_repository')
local schema_registry_opts = config_utils.get_schema_registry_opts(config_utils.get_config())
local ddl = require('ddl')
local routes_data = {}
local routes_key = {}

-- local http_client = require('http.client') --> new --TODO Optimization Http requests

local function get_schema_registry_opts()
    return schema_registry_opts
end

--TODO Refactor
local function get_key_schema(topic)
    if routes_key[topic] == nil then
        return false, 'ERROR: No schema key route found for topic ' .. topic
    else return true, routes_key[topic]
    end
end

local function get_data_schema(topic)
    if routes_data[topic] == nil then
        return false, error_repository.get_error_code('AVRO_SCHEMA_007',{topic = topic})
    else
        return true, routes_data[topic]
    end
end

local function init_routes()
    routes_key = config_utils.get_topics_x_schema_key(config_utils.get_config())
    routes_data = config_utils.get_topics_x_schema_data(config_utils.get_config())
    schema_registry_opts = config_utils.get_schema_registry_opts(config_utils.get_config())
end

local avro_schema_cache = {}

local function get_schema_registry_key_schema_name()
    if schema_registry_opts['key_schema_name'] == nil then
        return nil
    else return schema_registry_opts['key_schema_name']
    end
end

local function clear_cache()
    avro_schema_cache = {}
end

local function get_schema_from_schema_registry(server,port,schema_name,version)
    local url = server ..':' .. tostring(port) .. '/subjects/' .. schema_name .. '/versions/' .. version .. '/schema'
    log.info('INFO: Send get request to ' .. server .. ':' .. tostring(port))
    log.info('INFO: Trying to  obtain schema ' .. schema_name .. ' ,version ' .. version)
    local response = require('http.client').get(url,{timeout=0.5}) --TODO Optimization Http requests

    if(response['status'] ~= 200) then
        log.error(response['body'])
        return false,json.decode(response['body'])
    end
    log.info('INFO: Success request')

    return avro_schema.create(json.decode(response['body']))
end

local function get_latest_version(server,port,schema_name)
    local url = server ..':' .. tostring(port) .. '/subjects/' .. schema_name .. '/versions/'
    log.info('INFO: Send get request to ' .. server .. ':' .. tostring(port))
    log.info('INFO: Trying to  obtain latest version for ' .. schema_name)
    local response = require('http.client').get(url,{timeout=0.5}) --TODO Optimization Http requests

    if(response['status'] ~= 200) then
        log.error(response['body'])
        return false,json.decode(response['body'])
    end
    log.info('INFO: Success request')

    local versions = json.decode(response['body'])
    return true,fun.max(versions)
end

local function get_schema(schema_name)

    local ok,latest_version = get_latest_version(schema_registry_opts['host'],schema_registry_opts['port'],schema_name)

    if not ok then
        log.error(latest_version)
        return nil
    end

    local key = schema_name .. ':' .. latest_version

    if avro_schema_cache[key] ~= nil then
        return avro_schema_cache[key],key
    end

    local ok,schema = get_schema_from_schema_registry(schema_registry_opts['host'], schema_registry_opts['port'], schema_name, 'latest')

    if ok then
         avro_schema_cache[key] = schema
         return schema,key
    else
        log.error(schema)
        return nil
    end
end

local function extract_fields(fields)

    local type_mapping = {
        ['int'] = 'number',
        ['long'] = 'number',
        ['float'] = 'number',
        ['double'] = 'number',
        ['string'] = 'string',
        ['boolean'] = 'boolean',
        ['bytes'] = 'bin',

    }
    local format = {}
    for _, v in ipairs(fields) do
        local format_row = {}
        format_row['name'] = v['name']
        if type(v['type']) == 'string' then
            format_row['type'] = type_mapping[v['type']]
            format_row['is_nullable'] = false
        else if type(v['type']) == 'table' then
            format_row['is_nullable'] = true
            for _,v in ipairs(v['type']) do
                if v ~= 'null' then
                    format_row['type'] = v
                end
            end
        end
        end

        table.insert(format,format_row)
    end
    return format

end

local function generate_ddl(schema_name,table_name,opts)
    local engine = opts['engine'] or 'memtx'
    local is_local = opts['is_local'] or false
    local temporary = opts['temporary'] or false
    local schema = get_schema(schema_name)

    if schema == nil then
        return nil
    end

    local format = {}
    if schema['type'] == 'array' then
        format = extract_fields(schema['items']['fields'])
    else if schema['type'] == 'record' then
        format = extract_fields(schema['fields'])
    end
end
    return {
        [table_name] = {
            ['engine'] = engine,
            ['is_local'] = is_local,
            ['temporary'] = temporary,
            ['format'] = format
        }
    }
end

---get_avro_type_from_t_type - method, that's convert T type to Avro type.
---@param type string - tarantool type to convert to avro type.
---@return string - avro type.
local function get_avro_type_from_t_type(type)
    checks('string')
    local type_mapping = {
        ['unsigned'] = 'long',
        ['string'] = 'string',
        ['varbinary'] = 'bytes',
        ['integer'] = 'int',
        ['number'] = 'double',
        ['decimal'] = 'double',
        ['double'] =  'double'
    }

    return type_mapping[type]
end

---convert_space_format_to_avro_record_schema - method, that's generate avro schema for tarantool space.
---@param space_name string - name of space to generate avro schema.
---@param remove_bucket_id boolean - the optional flag that's show remove bucket_id field from schema or not.
---@return string - avro schema for tarantool space.
local function convert_space_format_to_avro_record_schema(space_name,remove_bucket_id)
    checks('string','?boolean')
    remove_bucket_id = remove_bucket_id or false
    local format = ddl.get_schema().spaces[space_name].format
    local result = {}
    result.name = 'record_' .. space_name
    result.type = 'record'
    result.fields = {}

    for _,v in ipairs(format) do
        if remove_bucket_id then
            if v.name == 'bucket_id' then
                goto continue
            end
        end

        local type
        if v.is_nullable then
            type = {[1] = get_avro_type_from_t_type(v.type) ,[2] = "null"}
        else
            type = get_avro_type_from_t_type(v.type)
        end

        table.insert(result.fields, {name = v.name,type = type})
        ::continue::
    end
    return json.encode(result)
end

---convert_sql_metadata_to_avro_record_schema - method, that's generate avro schema for tarantool sql query metadata.
---@param metadata table - metadata table from sql query.
---@param remove_bucket_id boolean - the optional flag that's show remove bucket_id field from schema or not.
---@return string - avro schema for tarantool sql query.
local function convert_sql_metadata_to_avro_record_schema(metadata,remove_bucket_id)
    checks('table','?boolean')
    remove_bucket_id = remove_bucket_id or false
    local result = {}
    result.name = 'record_' .. 'sql'
    result.type = 'record'
    result.fields = {}

    for _,v in ipairs(metadata) do
        if remove_bucket_id then
            if v.name == 'bucket_id' then
                goto continue
            end
        end

        local type
        type = {[1] = get_avro_type_from_t_type(v.type) ,[2] = "null"}
        table.insert(result.fields, {name = v.name,type = type})
        ::continue::
    end

    return json.encode(result)

end

return {
    get_schema = get_schema,
    clear_cache = clear_cache,
    get_schema_registry_key_schema_name = get_schema_registry_key_schema_name,
    init_routes = init_routes,
    get_key_schema = get_key_schema,
    get_data_schema = get_data_schema,
    get_schema_registry_opts = get_schema_registry_opts,
    generate_ddl = generate_ddl,
    convert_space_format_to_avro_record_schema = convert_space_format_to_avro_record_schema,
    convert_sql_metadata_to_avro_record_schema = convert_sql_metadata_to_avro_record_schema
}