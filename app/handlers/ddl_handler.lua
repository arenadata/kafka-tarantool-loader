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
--- Created by ashitov.
--- DateTime: 6/29/20 1:03 PM
---
local cartridge = require('cartridge')
local uuid = require('uuid')
local error_repository = require('app.messages.error_repository')
local json = require('json')

local function add_table_to_delete_batch(req)
    local table_name = req:stash('tableName')

    local batch_id = req:query_param('batchId') or uuid.str()

    if table_name == nil then
        return error_repository.return_http_response('API_DDL_TABLE_DELETE_BATCH_ADD_001')
    end

    local _,err = cartridge.rpc_call('app.roles.adg_state',
            'update_delete_batch_storage',
            {batch_id,{table_name}},
            {leader_only = true, timeout = 30})

    if err ~= nil then
        return error_repository.return_http_response('API_DDL_TABLE_DELETE_BATCH_ADD_003', {error = err})
    end

    return {status = 200, body = json.encode({batchId = batch_id})}
end


local function queued_prefix_delete(req)
    local prefix = req:stash('tablePrefix')
    if prefix == nil then
        return error_repository.return_http_response('API_DDL_TABLE_DELETE_BATCH_ADD_006',
                nil, json.encode({code = 'API_DDL_TABLE_DELETE_BATCH_ADD_006',
                                  message = 'ERROR: prefix param not found in the query.'}))
    end

    local waiting_operation_timeout = _G.api_timeouts:get_ddl_operation_timeout()
    local _,err = cartridge.rpc_call('app.roles.adg_state',
            'delayed_delete_prefix',
            { prefix, waiting_operation_timeout },
            { leader_only = true, timeout = waiting_operation_timeout + 10 })

    if err ~= nil then
        if err['code'] == 'API_DDL_QUEUE_004' then
            return error_repository.return_http_response(err['code'], nil, err)
        end
        return error_repository.return_http_response('API_DDL_QUEUE_003', nil, err)
    end

    return {status = 200, body = json.encode({})}
end

local function queued_tables_delete(req)
    local body = req:json()

    if body.tableList == nil then
        return error_repository.return_http_response('API_DDL_QUEUE_001')
    end

    if type(body.tableList) ~= 'table' then
        return error_repository.return_http_response('API_DDL_QUEUE_002')
    end

    local waiting_operation_timeout = _G.api_timeouts:get_ddl_operation_timeout()
    local _,err = cartridge.rpc_call('app.roles.adg_state',
            'delayed_delete',
            { body.tableList, waiting_operation_timeout },
            { leader_only = true, timeout = waiting_operation_timeout + 10 })
    if err ~= nil then
        if err['code'] == 'API_DDL_QUEUE_004' then
            return error_repository.return_http_response(err['code'], nil, err)
        end
        return error_repository.return_http_response('API_DDL_QUEUE_003', nil, err)
    end

    return {status = 200, body = json.encode({})}
end

local function queued_tables_create(req)
    local body = req:json()

    if body.spaces == nil then
        return error_repository.return_http_response('API_DDL_QUEUE_001')
    end

    if type(body.spaces) ~= 'table' then
        return error_repository.return_http_response('API_DDL_QUEUE_002')
    end

    local waiting_operation_timeout = _G.api_timeouts:get_ddl_operation_timeout()
    local _,err = cartridge.rpc_call('app.roles.adg_state',
            'delayed_create',
            { body.spaces, waiting_operation_timeout },
            { leader_only = true, timeout = waiting_operation_timeout + 10 })
    if err ~= nil then
        if err['code'] == 'API_DDL_QUEUE_004' then
            return error_repository.return_http_response(err['code'], nil, err)
        end
        return error_repository.return_http_response('API_DDL_QUEUE_003', nil, err)
    end

    return {status = 200, body = json.encode({})}
end


local function get_storage_space_schema(req)
    local body = req:json()

    if body.spaces == nil then
        return { status = 500, body = json.encode({error = "Empty spaces parameter"}) }
    end


    local res = _G.get_storage_space_schema(body.spaces)

    return {status = 200, body = res}
end

return {
    add_table_to_delete_batch = add_table_to_delete_batch,
    queued_tables_delete = queued_tables_delete,
    queued_tables_create = queued_tables_create,
    queued_prefix_delete = queued_prefix_delete,
    get_storage_space_schema = get_storage_space_schema
}