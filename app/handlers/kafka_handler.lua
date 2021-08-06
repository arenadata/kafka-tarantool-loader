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
--- DateTime: 6/15/20 10:42 AM
---
local error_repository = require('app.messages.error_repository')
local json = require('json')
local cartridge = require('cartridge')
local pool = require('cartridge.pool')
-- local log = require('log')
local avro_schema_lib = require('avro_schema')


local function is_cb_function_params_valid(function_name, function_params)

    local schema_str,schema_get_err = cartridge.rpc_call('app.roles.adg_state',
            'get_callback_function_schema',{function_name})

    if schema_get_err ~= nil then
        return false, schema_get_err
    end


    local is_schema_created, schema = avro_schema_lib.create(json.decode(schema_str))

    if not is_schema_created then
        return false, schema
    end

    local is_schema_valid, schema_validation_err = avro_schema_lib.validate(schema, function_params)

    if not is_schema_valid then
        return false, schema_validation_err
    end

    return is_schema_valid

end


local function subscribe_to_topic_on_cluster(req)
    --body
    local body = req:json()
    --check required params
    if body.topicName == nil then
        return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_001')
    end

    if body.maxNumberOfMessagesPerPartition == nil then
        return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_002')
    end

    if body.spaceNames == nil then
        return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_004')
    end


    -- check avro schema param
    local json_schema_string
    if body.avroSchema ~= nil then
        json_schema_string = json.encode(body.avroSchema)
    end

    local callback = body.callbackFunction
    --check params
    local cb_function_name = callback.callbackFunctionName
    local cb_function_param = callback.callbackFunctionParams

    if cb_function_param ~= nil and cb_function_name == nil then
        return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_005')
    end

    if cb_function_name ~= nil and cb_function_param ~= nil then
        local is_cb_f_valid, cb_f_validation_err = is_cb_function_params_valid(cb_function_name,cb_function_param)


        if not is_cb_f_valid then
            return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_006',{err = cb_f_validation_err})
        end
    end

    local max_idle_sec = body.maxIdleSecondsBeforeCbCall

    -- get all kafka connectors
    local kafka_connectors =  cartridge.rpc_get_candidates('app.roles.adg_kafka_connector',{leader_only = true})

    if #kafka_connectors == 0 then
        return error_repository.return_http_response('API_KAFKA_001')
    end

    local futures = {}

    for _,connector in ipairs(kafka_connectors) do
        local conn, err = pool.connect(connector, {wait_connected = 1.5})
        if conn == nil then
            return error_repository.return_http_response('API_KAFKA_002',{err=err})
        else
            local future = conn:call('subscribe_to_topic',
                    {body.topicName,body.spaceNames,
                     json_schema_string,body.maxNumberOfMessagesPerPartition,max_idle_sec,
                     cb_function_name,cb_function_param},{is_async=true})
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(30)
        local res,err = future:result()

        if res == nil then
            return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_003',
                    {topicName = body.topicName, err = err})
        end

        if res[2] ~= nil then
            return error_repository.return_http_response('API_KAFKA_SUBSCRIPTION_003',
                    {topicName = body.topicName, err=res[2]})
        end

    end

    return {status = 200}

end

local function unsubscribe_from_topic_on_cluster(req)
    local topic_name = req:stash('topicName')

    -- get all kafka connectors
    local kafka_connectors =  cartridge.rpc_get_candidates('app.roles.adg_kafka_connector',{leader_only = true})

    if #kafka_connectors == 0 then
        return error_repository.return_http_response('API_KAFKA_001')
    end


    local futures = {}

    for _,connector in ipairs(kafka_connectors) do
        local conn, err = pool.connect(connector, {wait_connected = 1.5})
        if conn == nil then
            return error_repository.return_http_response('API_KAFKA_002',{err=err})
        else
            local future = conn:call('unsubscribe_from_topic',
                    {topic_name},{is_async=true})
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(30)
        local res,err = future:result()

        if res == nil then
            return error_repository.return_http_response('API_KAFKA_UNSUBSCRIPTION_002',{topicName = topic_name, err=err})
        end

        if res[2] ~= nil then
            return error_repository.return_http_response('API_KAFKA_UNSUBSCRIPTION_002',{topicName = topic_name, err=res[2]})
        end

        --Suggest that false|nil returned if topic not found
        if res[1] == false then
            return error_repository.return_http_response('API_KAFKA_UNSUBSCRIPTION_001',{topicName = topic_name})
        end
    end

    return {status = 200}
end

local function dataload_from_topic_on_cluster(req)
    --body
    local body = req:json()

    --сheck params
    if body.topicName == nil then
        return error_repository.return_http_response('API_KAFKA_DATALOAD_001')
    end

    if body.spaces == nil then

        if type(body.spaces) ~= 'table' then
            return error_repository.return_http_response('API_KAFKA_DATALOAD_002')
        end

        return error_repository.return_http_response('API_KAFKA_DATALOAD_003')
    end


     --body.maxNumberOfMessagesPerPartition == nil

    -- check avro schema param
    local json_schema_string
    if body.avroSchema ~= nil then
        json_schema_string = json.encode(body.avroSchema)
    end

    local kafka_connectors =  cartridge.rpc_get_candidates('app.roles.adg_kafka_connector')

    if #kafka_connectors == 0 then
        return error_repository.return_http_response('API_KAFKA_001')
    end

    local futures = {}

    for _,connector in ipairs(kafka_connectors) do
        local conn, err = pool.connect(connector, {wait_connected = 1.5})
        if conn == nil then
            return error_repository.return_http_response('API_KAFKA_002',{err=err})
        else
            local future = conn:call('dataload_from_topic',
                    {body.topicName,body.spaces,body.maxNumberOfMessagesPerPartition,json_schema_string},{is_async=true})
            table.insert(futures,future)
        end
    end
    local messageCount = 0
    for _,future in ipairs(futures) do
        future:wait_result(30)
        local res,err = future:result()
        if res == nil then
            return error_repository.return_http_response('API_KAFKA_DATALOAD_004',
                    nil,json.encode({code = 'API_KAFKA_DATALOAD_004', message = err, messageCount = messageCount}))
        end

        if res[1] ~= true then
            return error_repository.return_http_response('API_KAFKA_DATALOAD_004',
                    nil, json.encode({code = 'API_KAFKA_DATALOAD_004', message = res[2], messageCount = messageCount}))
        end
        messageCount = messageCount + (res[3] or 0)
    end

    return {status = 200, body = json.encode({messageCount = messageCount})}

end

local function dataunload_query_to_topic_on_cluster(req)
    --body
    local body = req:json()

    --сheck params
    if body.query == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_001')
    end

    if body.topicName == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_002')
    end

    if body.avroSchema == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_005')
    end

    local avro_schema_string = json.encode(body.avroSchema)


    local maxNumberOfRowsPerMessage = body.maxNumberOfRowsPerMessage or 1000

    local ok,err =
    _G.execute_query_for_massive_select_to_kafka(body.topicName,body.query,maxNumberOfRowsPerMessage,'query'
        ,avro_schema_string)

    if not ok then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_004',{error = err})
    end

    return {status = 200}
end

local function dataunload_table_to_topic_on_cluster(req)
    --body
    local body = req:json()

    --сheck params
    if body.tableName == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_003')
    end

    if body.topicName == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_002')
    end

    if body.avroSchema == nil then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_005')
    end

    local avro_schema_string = json.encode(body.avroSchema)


    local maxNumberOfRowsPerMessage = body.maxNumberOfRowsPerMessage or 1000

    local filter = body.filter or '1 = 1'

    local query = 'select * from ' .. body.tableName .. ' where ' .. filter

    local ok,err =
    _G.execute_query_for_massive_select_to_kafka(body.topicName,query,maxNumberOfRowsPerMessage,body.tableName
    ,avro_schema_string)

    if not ok then
        return error_repository.return_http_response('API_KAFKA_DATAUNLOAD_004', {error = err})
    end

    return {status = 200}
end

local function register_kafka_callback_function(req)
    local body = req:json()

    if body.callbackFunctionName == nil then
        return error_repository.return_http_response('API_KAFKA_REG_CB_FUNC_001')
    end

    local callback_function_param_schema_string
    if body.callbackFunctionParamSchema ~= nil then
        callback_function_param_schema_string = json.encode(body.callbackFunctionParamSchema)
    end


    local _, err = cartridge.rpc_call('app.roles.adg_state','register_kafka_callback_function',
            {body.callbackFunctionName,body.callbackFunctionDesc,
             callback_function_param_schema_string},{leader_only=true})


    if err ~= nil then
        return error_repository.return_http_response('API_KAFKA_REG_CB_FUNC_002', {err = err})
    end

    return {status = 200}
end

-- luacheck: ignore req
local function get_kafka_callback_functions(req)

    local res,err = cartridge.rpc_call('app.roles.adg_state','get_callback_functions')

    if err ~= nil then
        return error_repository.return_http_response('API_KAFKA_GET_CB_FUNC_001', {err = err})

    end

    return  {status = 200, body = json.encode(res)}
end

local function delete_kafka_callback_function(req)
    local callback_function_name = req:stash('callbackFunctionName')

    local _, err = cartridge.rpc_call('app.roles.adg_state','delete_kafka_callback_function',
            {callback_function_name},{leader_only=true})


    if err ~= nil then
        return error_repository.return_http_response('API_KAFKA_DEL_CB_FUNC_001', {err = err})
    end

    return {status = 200}
end

return {
    subscribe_to_topic_on_cluster = subscribe_to_topic_on_cluster,
    unsubscribe_from_topic_on_cluster = unsubscribe_from_topic_on_cluster,
    dataload_from_topic_on_cluster = dataload_from_topic_on_cluster,
    dataunload_query_to_topic_on_cluster = dataunload_query_to_topic_on_cluster,
    dataunload_table_to_topic_on_cluster = dataunload_table_to_topic_on_cluster,
    register_kafka_callback_function = register_kafka_callback_function,
    get_kafka_callback_functions = get_kafka_callback_functions,
    delete_kafka_callback_function = delete_kafka_callback_function
}