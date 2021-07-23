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

local success_repository = require('app.messages.success_repository')
local error_repository = require('app.messages.error_repository')
local cartridge = require('cartridge')
local json = require('json')
local url_utils = require('app.utils.url_utils')
local metrics = require('app.metrics.metrics_storage')

--[[
10.18.84.10:8811/api/kafka/send_query/?_query=select * from employees&_topic=upload_employees&_batch_size=1000
]]

--TODO Refactor duplicate code

local function select_query_to_kafka_handler(req)

    local query = url_utils.url_decode(req:query_param('_query'))
    if query == nil then
        return error_repository.return_http_response('API_GET_QUERY_002')
    end


    local topic_name = url_utils.url_decode(req:query_param('_topic'))
    if topic_name == nil then
        return error_repository.return_http_response('API_GET_QUERY_002')
    end

    local batch_size = tonumber(req:query_param('_batch_size'))
    if batch_size == nil then
        return error_repository.return_http_response('API_GET_QUERY_003')
    end
    metrics.api_select_queries_total_counter:inc(1,{method = 'GET'})

    local ok,err = _G.execute_query_for_massive_select_to_kafka(topic_name,query,batch_size)

    --local ok,err = cartridge.rpc_call('app.roles.adg_output_processor','send_query_to_kafka',
    --{topic_name,query,{batch_size=batch_size}})

    if ok then return success_repository.return_http_response('ADG_OUTPUT_PROCESSOR_002')
    else
        if type(err) == 'string' then
            return error_repository.return_http_response(json.decode(err)['errorCode'],err)
        else return error_repository.return_http_response('ROLE_VALIDATE_CONFIG_ERR_001',err)
        end
    end
end


local function select_table_to_kafka_handler(req)
    local query_table = req:stash('table')
    if query_table == nil then
        return error_repository.return_http_response('API_GET_QUERY_001')
    end

    local filter = url_utils.url_decode(req:query_param('_filter'))

    local topic_name = url_utils.url_decode(req:query_param('_topic'))

    if topic_name == nil then
        return error_repository.return_http_response('API_GET_QUERY_002')
    end
    local batch_size = tonumber(req:query_param('_batch_size'))

    if batch_size == nil then
        return error_repository.return_http_response('API_GET_QUERY_003')
    end

    local schema_name = url_utils.url_decode(req:query_param('_schema_name'))

    if schema_name == nil then
        return error_repository.return_http_response('API_GET_QUERY_004')
    end

    metrics.api_select_queries_total_counter:inc(1,{method = 'GET'})

    local ok,err = cartridge.rpc_call('app.roles.adg_output_processor','send_table_to_kafka',
    {topic_name,query_table,filter,{batch_size=batch_size,schema_name=schema_name}})
    if ok then return success_repository.return_http_response('ADG_OUTPUT_PROCESSOR_002')
    else
        if type(err) == 'string' then
            return error_repository.return_http_response(json.decode(err)['errorCode'],err)
        else return error_repository.return_http_response('ROLE_VALIDATE_CONFIG_ERR_001',err)
        end
    end

end


return {
    select_table_to_kafka_handler = select_table_to_kafka_handler,
    select_query_to_kafka_handler = select_query_to_kafka_handler
}