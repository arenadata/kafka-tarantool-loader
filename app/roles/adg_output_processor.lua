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

local cartridge = require('cartridge')
local prometheus = require('metrics.plugins.prometheus')
local log = require('log')
local checks = require('checks')
local avro_utils = require('app.utils.avro_utils')
local avro_schema_utils = require('app.utils.avro_schema_utils')
local misc_utils = require('app.utils.misc_utils')
local sql_select =require('app.utils.sql_select')
local schema_utils = require('app.utils.schema_utils')
local json = require('json')
local fiber = require('fiber')
local error_repository = require('app.messages.error_repository')
local success_repository = require('app.messages.success_repository')
local vshard = require('vshard')
local metrics = require('app.metrics.metrics_storage')
local yaml = require('yaml')
local validate_utils = require('app.utils.validate_utils')
local bin_avro_utils = require('app.utils.bin_avro_utils')

local role_name = 'app.roles.adg_output_processor'

local garbage_fiber = nil

_G.test_msg_to_kafka = nil
_G.send_simple_msg_to_kafka = nil
_G.send_query_to_kafka = nil
_G.send_table_to_kafka = nil
_G.get_metric = nil
_G.send_query_to_kafka_with_plan = nil

local key_schema = [[
{"type":"record","name":"DtmQueryResponseMetadata","namespace":"ru.ibs.dtm.common.model",
"fields":[{"name":"tableName","type":"string"},{"name":"streamNumber","type":"int"},
{"name":"streamTotal","type":"int"},{"name":"chunkNumber","type":"int"},{"name":"isLastChunk","type":"boolean"}]}
]]

local function send_simple_msg_to_kafka(topic_name, key, value)
    local msg = {}
    table.insert(msg,{key=key,value=value})
    log.info('INFO: Sending data into kafka_connector')
    local _,err = cartridge.rpc_call('app.roles.adg_kafka_connector','send_messages_to_kafka',
            {topic_name,msg})
    if err ~= nil then
        return false,err
    end

    return true,nil
end


local function stop()
    garbage_fiber:cancel()
    return true
end

local function validate_config(conf_new, conf_old)
    if type(box.cfg) ~= 'function' and not box.cfg.read_only then
        local schema_registry_opts = yaml.decode(conf_new['kafka_schema_registry.yml'] or [[]]) or {['host'] = 'localhost',['port'] = 8081}

        local is_schema_registry_opts_ok,schema_registry_opts_err = validate_utils.check_schema_registry_opts(schema_registry_opts)

        if not is_schema_registry_opts_ok then
            return false,schema_registry_opts_err
        end
    end
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    if opts.is_master and pcall(vshard.storage.info) == false then
        schema_utils.drop_all()
    end
    schema_utils.init_schema_ddl()
    avro_schema_utils.init_routes()
    error_repository.init_error_repo('en')
    success_repository.init_success_repo('en')
    return true
end



local function send_messages_to_kafka(topic_name, messages, opts)
    checks('string','table', 'table')
    log.info('INFO: Sending ' .. misc_utils.table_length(messages) .. ' rows into kafka_connector')
    local _,err = cartridge.rpc_call('app.roles.adg_kafka_connector','send_messages_to_kafka',
                                        {topic_name,messages,opts})
    if err ~= nil then
        error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_002',{topic_name=topic_name,opts=opts,err=err})
        return false
    end
    return true
    --TODO Add error handling
end

---send_query_to_kafka_with_plan
---@param replica_uuid string -
---@param plan table
---@param stream_number number
---@param stream_total number
---@param params table
---@return boolean|string
local function send_query_to_kafka_with_plan(replica_uuid,plan,stream_number,stream_total,params)
    checks('string','table','number','number', {
        topic_name = 'string',
        query = 'string',
        avro_schema = '?string',
        batch_size = '?number',
        table_name = '?string'
    })

    local query = params['query']
    local topic_name = params['topic_name']

    if next(plan) == nil then
        local _,key = bin_avro_utils.encode(key_schema,{{
                                                                 tableName = params.table_name or '',
                                                                 streamNumber = stream_number-1,
                                                                 streamTotal = stream_total,
                                                                 chunkNumber = 0,
                                                                 isLastChunk = true
                                                             }},true)

        local is_message_sended, message_send_err = send_simple_msg_to_kafka(topic_name,key,nil)

        if is_message_sended then
            success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_001')
        else return false, error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_002',
                {topic_name=topic_name,
                 key=key, error = message_send_err
                })
        end
    end

    local replicas = vshard.router.routeall()
    local replica = replicas[replica_uuid]

    -- Get avro schema from query
    local schema_query = query .. ' limit 1'
    local schema_row = replica:callbre('execute_sql', {schema_query}, {is_async= false})
    local avro_schema = params['avro_schema'] or
            avro_schema_utils.convert_sql_metadata_to_avro_record_schema(schema_row.metadata)
    --

    local stream_query = query .. ' order by 1 limit ? offset ?'

    local chunks = {}
    replica:callbre('prep_sql', {stream_query}, {is_async= false})

    local futures_send = 0
    for chunk_number,v in ipairs(plan) do --package
        if futures_send == 10 then
            fiber.sleep(500 / #plan) --back pressure
            futures_send = 0
        end
        local future = replica:callbre(
                'execute_sql',
                {stream_query,{v['limit'],v['offset']}},
                {is_async=true, timeout=60}
        )
        chunks[chunk_number] = future
        futures_send = futures_send + 1
    end

    local chunk_total = #chunks
    local is_last_chunk = false

    for chunk_number, future in ipairs(chunks) do

        if chunk_number == chunk_total then
            is_last_chunk = true
        end
        future:wait_result(360)
        local res, err = future:result()

        if res == nil then
            return false, error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_001', {desc=err,
                                                                                       stream_number=stream_number,
                                                                                       chunk_number=chunk_number,
                                                                                       is_last_chunk =is_last_chunk})
        end

        --Remove bucket_id
        --[[
        if schema_name ~= nil then
            local ok,bucket_id_c = pcall(sql_select.get_bucket_id_column_number(res[1]))
            if bucket_id_c ~= nil then
                for _,v in ipairs(res[1]['rows']) do
                    v[bucket_id_c] = nil
                end
            end
        end]]

        local is_generate, data = bin_avro_utils.encode(avro_schema,res[1]['rows'],true)


        if not is_generate then
            return false, error_repository.get_error_code('AVRO_SCHEMA_003', {desc=data})
        end

        local _,key = bin_avro_utils.encode(key_schema, {{
                                                                  tableName = params.table_name or '',
                                                                  streamNumber = stream_number-1,
                                                                  streamTotal = stream_total,
                                                                  chunkNumber = chunk_number-1,
                                                                  isLastChunk = is_last_chunk
                                                              }},true)
        futures_send = 0
        if futures_send == 10 then
            fiber.sleep(500 / #plan)
            futures_send = 0
        end
        local is_message_sended, message_send_err = send_simple_msg_to_kafka(topic_name,key,data)

        futures_send = futures_send + 1

        if is_message_sended then
            success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_001')
        else return false, error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_002',
                {topic_name=topic_name,
                 key=key, error = message_send_err
                })
        end
    end

    return true, success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_002')
end

local function send_query_to_kafka(topic_name, query, opts)

    if not string.match(string.lower(query), "^%s*select%s+") then
        return false,error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_003',{query=query})
    end

    local batch_size = opts['batch_size'] or 1000
    local schema_name = opts['schema_name'] or nil
    local schema = nil
    local ok,methods = nil,nil
    local is_generate, data = nil, nil
    
    if schema_name ~= nil then
        schema = avro_schema_utils.get_schema(schema_name)

        if schema == nil then
            return false, error_repository.get_error_code('AVRO_SCHEMA_001',{schema_registry=avro_schema_utils.get_schema_registry_opts,
                                                                            schema_name=schema_name})
        end

        ok,methods = avro_utils.compile_avro_schema(schema)

        if not ok then
            return false, error_repository.get_error_code('AVRO_SCHEMA_002',{schema_name=schema_name,methods=methods})
        end
    end

    local replicas, err = sql_select.get_replicas(
        query, {})

    if err ~= nil then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {query=query,desc=err})
    end

    local split_query = {}

    for _,cand in pairs(replicas) do
        local row_cnt, row_cnt_err = cand:callbre(
            'execute_sql',
            {string.format("select count(*) from (%s);",query)},
            {is_async=false}
        )
        if row_cnt_err~= nil then
            return false, error_repository.get_error_code('VSTORAGE_SQL_SELECT_001', {sql_err=row_cnt_err,query=query})
        end

        local split = misc_utils.generate_limit_offset(row_cnt['rows'][1][1],batch_size)
        split_query[cand] = split
    end


    
    local stream_query = query .. ' order by 1 limit ? offset ?'


    local streams = {}

    local stream_number = 0
    for cand,split in pairs(split_query) do --stream
        stream_number = stream_number + 1
        streams[stream_number] = {}
        cand:callbre('prep_sql', {stream_query}, {is_async= false})
        for chunk_number,v in ipairs(split) do --package
            local future = cand:callbre(
                'execute_sql',
                {stream_query,{v['limit'],v['offset']}},
                {is_async=true, timeout=360}
            )
            fiber.sleep(0.01)
            streams[stream_number][chunk_number] = future
        end
    end

    for stream_number, futures in ipairs(streams) do
        local chunk_total = #futures
        local is_last_chunk = false
        for chunk_number, future in ipairs(futures) do

            if chunk_number == chunk_total then
                 is_last_chunk = true
            end
            future:wait_result(360)
            local res, err = future:result()
            
            if res == nil then
                return false, error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_001', {desc=err,
                                                                                        stream_number=stream_number,
                                                                                        chunk_number=chunk_number,
                                                                                        is_last_chunk =is_last_chunk})
            end

            --Remove bucket_id
            if schema_name ~= nil then
                local ok,bucket_id_c = pcall(sql_select.get_bucket_id_column_number(res[1]))
                if bucket_id_c ~= nil then
                    for _,v in ipairs(res[1]['rows']) do
                        v[bucket_id_c] = nil
                    end
                end
            end

            if schema_name ~= nil then
                 is_generate, data = methods.unflatten({res[1]['rows']})
            else  is_generate, data = true, res[1]['rows']
            end

            if not is_generate then
                return false, error_repository.get_error_code('AVRO_SCHEMA_003', {desc=data})
            end

            local key = json.encode({
                streamNumber = stream_number-1,
                streamTotal = #streams,
                chunkNumber = chunk_number-1,
                isLastChunk = is_last_chunk
            })

            local result = send_messages_to_kafka(topic_name,{[1] = {key = key
            , value=json.encode(data)}},{is_async=true})
            if result then
                success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_001')
            else return false, error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_002',
                                                                            {topic_name=topic_name,
                                                                            key=key
                                                                            })
                                                                        end
        end
    end
    return true, success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_002')

end


local function send_table_to_kafka(topic_name,table,filter,opts)
    local ok,err
    if filter == nil then
         ok,err = send_query_to_kafka(topic_name,string.format('select * from %s',table),opts)
    else  ok,err = send_query_to_kafka(topic_name,string.format('select * from %s where %s',table,filter),opts) --TODO SQL injections ????????????
    end

    return ok,err
end

local function test_msg_to_kafka()
    send_simple_msg_to_kafka('input_test','2','value')
end


local function get_metric()
    return metrics.export(role_name)
end


local function init(opts)

    _G.test_msg_to_kafka = test_msg_to_kafka
    _G.send_simple_msg_to_kafka = send_simple_msg_to_kafka
    _G.send_query_to_kafka = send_query_to_kafka
    _G.send_table_to_kafka = send_table_to_kafka
    _G.send_query_to_kafka_with_plan = send_query_to_kafka_with_plan

    if opts.is_master then
    end

    garbage_fiber = fiber.create(
        function() while true do collectgarbage('step', 20);
            fiber.sleep(0.2) end end
    )

    garbage_fiber:name('GARBAGE_COLLECTOR_FIBER')
    
    _G.get_metric = get_metric
    
    local httpd = cartridge.service_get('httpd')
    httpd:route({method='GET', path = '/metrics'}, prometheus.collect_http)
    
    return true
end




return {
    role_name = role_name,
    init = init,
    stop = stop,
    validate_config = validate_config,
    apply_config = apply_config,
    send_simple_msg_to_kafka = send_simple_msg_to_kafka,
    test_msg_to_kafka = test_msg_to_kafka,
    send_messages_to_kafka = send_messages_to_kafka,
    send_query_to_kafka = send_query_to_kafka,
    send_table_to_kafka = send_table_to_kafka,
    send_query_to_kafka_with_plan = send_query_to_kafka_with_plan,
    get_metric = get_metric,
    dependencies = {'cartridge.roles.vshard-router'}

}
