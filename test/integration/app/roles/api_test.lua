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
--- DateTime: 3/10/20 7:16 PM
---
local t = require('luatest')
local g = t.group('integration_api')
local g2= t.group('api_delta_processing_test')
local g3 = t.group('api.drop_space_on_cluster')
local g4 = t.group('api.kafka_connector_api_test')
local g5 = t.group('api.storage_ddl_test')
local g6 = t.group('api.delete_scd_sql')
local g7 = t.group('api.get_scd_table_checksum')
local g8 = t.group('api.truncate_space_on_cluster')

local checks = require('checks')
local helper = require('test.helper.integration')
local cluster = helper.cluster
local fiber =require('fiber')

local file_utils = require('app.utils.file_utils')


local function assert_http_json_request(method, path, body, expected)
    checks('string', 'string', '?table', 'table')
    local response = cluster:server('api-1'):http_request(method, path, {
        json = body,
        headers = {["content-type"]="application/json; charset=utf-8"},
        raise = false
    })
    if expected.body then
        t.assert_equals(response.json, expected.body)
        return response.json
    end
    t.assert_equals(response.status, expected.status)

    return response
end

local schema = file_utils.read_file('test/integration/data/schema_ddl.yml')


g2.before_each(function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    storage1:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage1:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
    storage1:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_HOT'})
end)


g4.before_each(function()
    local kafka = cluster:server('kafka_connector-1').net_box
    kafka:call('box.execute', {'truncate table _KAFKA_TOPIC'})
end)

g6.before_each(function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    storage1:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_HOT'})
end)

g7.before_each(function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    storage1:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage1:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage1:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage2:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
end)

g.test_set_ddl = function ()
    local net_box = cluster:server('api-1').net_box
    local res, err = net_box:call('set_ddl', {schema})
    t.assert_equals(err, nil)
end

g.test_simple_insert_query = function ()
    local storage = cluster:server('master-1-1').net_box
    --[[
    local net_box = cluster:server('api-1').net_box
    local res, err = net_box:call(
            'query',
            {
                "INSERT INTO USER1 (id, first_name, last_name, email) VALUES (1, ?, ?, 'johndoe@example.com') values (4, 'Ivan', 'Ivanov', 'ivan@example.com')",
                {'John', 'Doe'}
            }
    )

    t.assert_equals(err, nil)

    t.assert_equals(res, {row_count=2})]]

    storage.space.USER1:replace({1, 'John', 'Doe', 'johndoe@example.com', 7729})
end

g.test_simple_select_query = function ()

    local net_box = cluster:server('api-1').net_box
    local res, err = net_box:call(
            'query',
            {"select * from USER1 where id = ?", {1}}
    )

    t.assert_equals(err, nil)


    t.assert_equals(res.rows,
            {{1, 'John', 'Doe', 'johndoe@example.com', 7729}})

end

g.test_cluster_schema_update = function()
    local net_box = cluster:server('api-1').net_box
    local storage = cluster:server('master-1-1')

    local res,err = storage.net_box:eval([[
    s = box.schema.space.create('user2');

    s:format({
          {name = 'id', type = 'unsigned'},
          {name = 'band_name', type = 'string'},
          {name = 'year', type = 'unsigned'}
          });

    s:create_index('primary', {
    type = 'hash',
    parts = {'id'}
    });
    ]])

    t.assert_equals(err, nil)

    local storage_uri = tostring(storage.advertise_uri)

    local res, err = net_box:call(
            'sync_ddl_schema_with_storage',
            {storage_uri},{timeout=30}) -- No support redundant argument \"nullable_action\""
    t.assert_equals(err, nil)
    local new_config = cluster:download_config()

    local user2_schema =  { user2 = {
        engine = "memtx",
        format = {
            {is_nullable = false, name = "id", type = "unsigned"},
            {is_nullable = false, name = "band_name", type = "string"},
            {is_nullable = false, name = "year", type = "unsigned"},
        },
        indexes = {
            {
                name = "primary",
                parts = {{is_nullable = false, path = "id", type = "unsigned"}},
                type = "HASH",
                unique = true,
            },
        },
        is_local = false,
        temporary = false
    }}

    t.assert_covers(new_config.schema.spaces,user2_schema)

end

g.test_api_get_config = function()
    local net_box = cluster:server('api-1').net_box

    local res = assert_http_json_request('GET', '/api/get_config', nil, {body = file_utils.read_file('/test/integration/data/api/get_config_response.json'),
                                                                         status = 200})

end

g.test_api_metrics_get_all = function()
    local net_box = cluster:server('api-1').net_box
    --TODO Add test
end

g2.test_100k_transfer_data_to_historical_table_on_cluster = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    local api = cluster:server('api-1').net_box

    local function datagen(storage,number_of_rows,version) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_TRANSFER:insert{i,version,1,1,'Test','IT','Test',300,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,100000,1)
    datagen(storage2,100000,1)

    datagen(storage1,100000,2)
    datagen(storage2,100000,2)


    local res,err = api:call('transfer_data_to_historical_table_on_cluster',{'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2} )
    local cnt1_1 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_2 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})

    local cnt2_1 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_2 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,100000)
    t.assert_equals(cnt1_2,100000)
    t.assert_equals(cnt2_1,100000)
    t.assert_equals(cnt2_2,100000)
end



g2.test_100k_transfer_data_to_historical_scd_on_cluster = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    local api = cluster:server('api-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,100000)
    datagen(storage2,100000)



    local res,err = api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT','EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )

    local cnt1_1 = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})

    local cnt2_1 = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,100000)
    t.assert_equals(cnt1_3,0)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,100000)
    t.assert_equals(cnt2_3,0)


    datagen(storage1,100000)
    datagen(storage2,100000)

    local res,err = api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT','EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2} )

    local cnt1_1 = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})

    local cnt2_1 = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,100000)
    t.assert_equals(cnt1_3,100000)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,100000)
    t.assert_equals(cnt2_3,100000)

end

g2.test_rest_api_transfer_data_to_historical_table_on_cluster = function ()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    local api = cluster:server('api-1').net_box

    local function datagen(storage,number_of_rows,version) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_TRANSFER:insert{i,version,1,1,'Test','IT','Test',300,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,1000,1)
    datagen(storage2,1000,1)

    datagen(storage1,1000,2)
    datagen(storage2,1000,2)


    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_historical_table?_actual_data_table_name=EMPLOYEES_TRANSFER&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                message = "INFO: Transfer data to a historical table on cluster done",
                status = "ok",
            }
            , status = 200})

    local cnt1_1 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_2 = storage1:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})

    local cnt2_1 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_2 = storage2:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(cnt1_1,1000)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt2_1,1000)
    t.assert_equals(cnt2_2,1000)


end


g2.test_rest_api_error_transfer_data_to_historical_table_on_cluster = function ()

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_historical_table?&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                error = "ERROR: _actual_data_table_name param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001",
                status = "error",
            }
            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_historical_table?_actual_data_table_name=EMPLOYEES_TRANSFER&_delta_number=2',
            nil, {body = {
                error = "ERROR: _historical_data_table_name param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_002",
                status = "error",
            }
            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_historical_table?_actual_data_table_name=EMPLOYEES_TRANSFER&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST',
            nil, {body = {
                error = "ERROR: _delta_number param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_003",
                status = "error",
            }

            , status = 400})


    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_historical_table?_actual_data_table_name=EMPLOYEES_TRANSFER2&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                error = "ERROR: No such space",
                errorCode = "STORAGE_001",
                opts =  {
                    error = "ERROR: No such space",
                    errorCode = "STORAGE_001",
                    opts = {space = "EMPLOYEES_TRANSFER2"},
                    status = "error",
                },
                status = "error",
            }
            , status = 400})
end


g2.test_rest_api_error_transfer_data_to_scd_table_on_cluster = function ()

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                error = "ERROR: _actual_data_table_name param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001",
                status = "error",
            }
            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_actual_data_table_name=EMPLOYEES_TRANSFER&_delta_number=2',
            nil, {body = {
                error = "ERROR: _historical_data_table_name param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_002",
                status = "error",
            }
            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_actual_data_table_name=EMPLOYEES_TRANSFER&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST',
            nil, {body = {
                error = "ERROR: _delta_number param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_003",
                status = "error",
            }

            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_actual_data_table_name=EMPLOYEES_TRANSFER2&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                error = "ERROR: _stage_data_table_name param in query not found",
                errorCode = "API_ETL_TRANSFER_DATA_TO_SCD_TABLE_001",
                status = "error",
            }

            , status = 400})

    local res = assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_actual_data_table_name=EMPLOYEES_TRANSFER2&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil, {body = {
                error = "ERROR: No such space",
                errorCode = "STORAGE_001",
                opts =  {
                    error = "ERROR: No such space",
                    errorCode = "STORAGE_001",
                    opts = {space = "EMPLOYEES_TRANSFER2"},
                    status = "error",
                },
                status = "error",
            }
            , status = 400})
end

g3.test_drop_existing_spaces_on_cluster = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    local api = cluster:server('api-1').net_box

    local res,err = api:call('drop_space_on_cluster', {'DROP_TABLE',false})
    t.assert_equals(res,true)
    t.assert_equals(err,nil)
    t.assert_equals(storage1.space.DROP_TABLE, nil)
    t.assert_equals(storage2.space.DROP_TABLE, nil)
end

g4.test_subscription_api = function()
    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {maxNumberOfMessagesPerPartition = 10000}, {body = {
                code = "API_KAFKA_SUBSCRIPTION_001",
                message = "ERROR: topicName param not found in the query.",
            }
            , status = 400})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = '123'}, {body = {
                code = "API_KAFKA_SUBSCRIPTION_002",
                message = "ERROR: maxNumberOfMessagesPerPartition param not found in the query.",
            }
            , status = 400})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = '123', maxNumberOfMessagesPerPartition = 10000}, { status = 200})


    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = '1234',
             maxNumberOfMessagesPerPartition = 10000,
             avroSchema =  { type = "long" }}, { status = 200})

end


g4.test_unsubscription_api = function()

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/1234',
            nil, { status = 404})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = '123', maxNumberOfMessagesPerPartition = 10000}, { status = 200})

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/123',
            nil, { status = 200})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = 'a', maxNumberOfMessagesPerPartition = 10000}, { status = 200})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = 'b', maxNumberOfMessagesPerPartition = 10000}, { status = 200})

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = 'c', maxNumberOfMessagesPerPartition = 10000}, { status = 200})

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/a',
            nil, { status = 200})

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/b',
            nil, { status = 200})

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/c',
            nil, { status = 200})

end

local function string_function()
    local random_number
    local random_string
    random_string = ""
    for x = 1,10,1 do
        random_number = math.random(65, 90)
        random_string = random_string .. string.char(random_number)
    end
    return random_string
end

local function gen_sub_unsub(topic_name)
    local s = assert_http_json_request('POST',
            '/api/v1/kafka/subscription',
            {topicName = topic_name, maxNumberOfMessagesPerPartition = 10000}, { status = 200})

    local uns = assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/' .. topic_name,
            nil, { status = 200})
    return pcall(t.assert_equals,s.status == 200 and uns.status == 200,true)
end

g4.test_sub_unsub = function()
    local fibers = {}
    for i=1,100 do
        local f = fiber.new(gen_sub_unsub,string_function())
        f:set_joinable(true)
        table.insert(fibers,f)
    end

    for _,v in ipairs(fibers) do
        local r,v = v:join()
        t.assert_equals(r,true)
        t.assert_equals(v,true)
    end
end

g4.test_dataload_api = function()

end

g5.test_get_storage_space_schema = function()
    local api = cluster:server('api-1').net_box

    local res = api:call('get_storage_space_schema', {{'RANDOM_SPACE'}})
    t.assert_equals(type(res),'string')
    t.assert(string.len(res) > 0)

    local res2 = api:call('get_storage_space_schema', {{'EMPLOYEES_HOT'}})
    t.assert_equals(type(res2),'string')
    t.assert(string.len(res2) > 0)
end

g6.test_delete_scd_sql_on_cluster = function()
    local api = cluster:server('api-1').net_box
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end


    datagen(storage1,1000)
    datagen(storage2,1000)

    local cnt1_before = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_before = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})

    t.assert_equals(cnt1_before,1000)
    t.assert_equals(cnt2_before,1000)

    local _,err_truncate = api:call('delete_data_from_scd_table_sql_on_cluster',{'EMPLOYEES_HOT'} )


    t.assert_equals(err_truncate, nil)

    local cnt1_after_truncate = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_after_truncate = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})

    t.assert_equals(cnt1_after_truncate,0)
    t.assert_equals(cnt2_after_truncate,0)

    datagen(storage1,10000)
    datagen(storage2,10000)

    local _,err_delete_half = api:call('delete_data_from_scd_table_sql_on_cluster',{'EMPLOYEES_HOT', '"id" >= 5001'} )

    t.assert_equals(err_delete_half, nil)

    local cnt1_after_half_1 = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_after_half_2 = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})
    t.assert_equals(cnt1_after_half_1,5000)
    t.assert_equals(cnt2_after_half_2,5000)

    local _,err_delete_another = api:call('delete_data_from_scd_table_sql_on_cluster',{'EMPLOYEES_HOT', [["name" = '123']]} )

    t.assert_equals(err_delete_another, nil)


    local cnt1_after_another_1 = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_after_another_2 = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})
    t.assert_equals(cnt1_after_another_1,0)
    t.assert_equals(cnt2_after_another_2,0)

end

g6.test_delete_scd_sql_on_cluster_rest  = function ()

    local api = cluster:server('api-1').net_box
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,1000)
    datagen(storage2,1000)


    local cnt1_before = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_before = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})

    t.assert_equals(cnt1_before,1000)
    t.assert_equals(cnt2_before,1000)

    assert_http_json_request('POST',
    '/api/etl/delete_data_from_scd_table',
    {spaceName = 'c'}, { status = 500})


    assert_http_json_request('POST',
    '/api/etl/delete_data_from_scd_table',
    {spaceName = 'EMPLOYEES_HOT',whereCondition = [["name1234" = '123']]}, { status = 500})

    assert_http_json_request('POST',
    '/api/etl/delete_data_from_scd_table',
    {spaceName = 'EMPLOYEES_HOT',whereCondition = [["name" = '123']]}, { status = 200})

    local cnt1_after_truncate = storage1:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_after_truncate = storage2:call('storage_space_count', {'EMPLOYEES_HOT'})

    t.assert_equals(cnt1_after_truncate,0)
    t.assert_equals(cnt2_after_truncate,0)
end


g7.test_get_scd_checksum_on_cluster_wo_columns = function()
    local api = cluster:server('api-1').net_box
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end


    datagen(storage1,1000)
    datagen(storage2,1000)
    local is_gen, res = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen, true)
    t.assert_equals(res, 0)

    api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT','EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1})

    local is_gen2, res2 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen2,true)
    t.assert_equals(res2,2000)

    datagen(storage1,1000)
    datagen(storage2,1000)
    api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT','EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2})

    local is_gen3, res3 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen3,true)
    t.assert_equals(res3,2000)

    local is_gen4, res4 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',2})
    t.assert_equals(is_gen4,true)
    t.assert_equals(res4,2000)

end

g7.test_get_scd_checksum_on_cluster_w_columns = function()
    local api = cluster:server('api-1').net_box
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,1000)
    datagen(storage2,1000)
    local is_gen, res = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen,true)
    t.assert_equals(res,0)
    api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local is_gen2, res2 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen2,true)
    t.assert_equals(res2,1181946280889 * 2)
    datagen(storage1,1000)
    datagen(storage2,1000)
    api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local is_gen3, res3 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen3,true)
    t.assert_equals(res3,1181946280889 * 2)

    local is_gen4, res4 = api:call('get_scd_table_checksum_on_cluster', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',2,{'id','sysFrom'}})
    t.assert_equals(is_gen4,true)
    t.assert_equals(res4,1180041276702 * 2)

end


g7.test_get_scd_checksum_on_cluster_rest  = function ()

    local api = cluster:server('api-1').net_box
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    local function datagen(storage,number_of_rows) --TODO Move in helper functions
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100} --TODO Bucket_id fix?
        end
    end

    datagen(storage1,1000)
    datagen(storage2,1000)

    assert_http_json_request('POST',
    '/api/etl/get_scd_table_checksum',
    {historicalDataTableName = 'c', sysCn = 666}, { status = 500})

    assert_http_json_request('POST',
    '/api/etl/get_scd_table_checksum',
    {actualDataTableName = 'c', sysCn = 666}, { status = 500})

    assert_http_json_request('POST',
    '/api/etl/get_scd_table_checksum',
    {actualDataTableName = 'c', historicalDataTableName = 'c'}, { status = 500})


    assert_http_json_request('POST',
    '/api/etl/get_scd_table_checksum',
    {actualDataTableName = 'EMPLOYEES_TRANSFER', historicalDataTableName = 'EMPLOYEES_TRANSFER_HIST', sysCn = 1}, { status = 200})

    api:call('transfer_data_to_scd_table_on_cluster',{'EMPLOYEES_HOT','EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1})

    assert_http_json_request('POST',
    '/api/etl/get_scd_table_checksum',
    {actualDataTableName = 'EMPLOYEES_TRANSFER', historicalDataTableName = 'EMPLOYEES_TRANSFER_HIST', sysCn = 1}, { status = 200, body = {checksum = 2000}})

end

g8.test_truncate_existing_spaces_on_cluster = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box
    local api = cluster:server('api-1').net_box

    -- refresh net.box schema metadata
    storage1:eval('return true')
    storage2:eval('return true')

    local function datagen(storage,number_of_rows,bucket_id)
        for i=1, number_of_rows, 1 do
            storage.space.TRUNCATE_TABLE:insert{i, bucket_id}
        end
    end

    datagen(storage1,1000,1)
    datagen(storage2,1000,2)

    local res, err = api:call('truncate_space_on_cluster', {'TRUNCATE_TABLE',false})

    local count_1 = storage1:call('storage_space_count', {'TRUNCATE_TABLE'})
    local count_2 = storage1:call('storage_space_count', {'TRUNCATE_TABLE'})

    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(count_1,0)
    t.assert_equals(count_2,0)
end
