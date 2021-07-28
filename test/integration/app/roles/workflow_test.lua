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

local t = require('luatest')
local tnt_kafka = require('kafka')
local fiber = require('fiber')
local log = require('log')
local bin_avro_utils = require('app.utils.bin_avro_utils')
local file_utils = require('app.utils.file_utils')

local g = t.group('workflow_api')
local helper = require('test.helper.integration')
local cluster = helper.cluster

local function assert_http_json_request(method, path, body, expected)
    checks('string', 'string', '?table', 'table')
    local response = cluster:server('api-1'):http_request(method, path, {
        json = body,
        headers = { ["content-type"] = "application/json; charset=utf-8" },
        raise = false
    })
    if expected.body then
        t.assert_equals(response.json, expected.body)
        return response.json
    end
    t.assert_equals(response.status, expected.status)

    return response
end

g.before_each(function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    storage1:call('box.execute', { 'truncate table EMPLOYEES_TRANSFER_HIST' })
    storage1:call('box.execute', { 'truncate table EMPLOYEES_TRANSFER' })
    storage1:call('box.execute', { 'truncate table EMPLOYEES_HOT' })
    storage2:call('box.execute', { 'truncate table EMPLOYEES_TRANSFER_HIST' })
    storage2:call('box.execute', { 'truncate table EMPLOYEES_TRANSFER' })
    storage2:call('box.execute', { 'truncate table EMPLOYEES_HOT' })

    local kafka = cluster:server('kafka_connector-1').net_box
    kafka:call('box.execute', { 'truncate table _KAFKA_TOPIC' })
end)

g.test_subscribe_unsubscribe_simple = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription', {
                topicName = "EMPLOYEES",
                spaceNames = { "EMPLOYEES_HOT" },
                avroSchema = nil,
                maxNumberOfMessagesPerPartition = 100,
                maxIdleSecondsBeforeCbCall = 100,
                callbackFunction = {
                    callbackFunctionName = "transfer_data_to_scd_table_on_cluster_cb",
                    callbackFunctionParams = {
                        _space = "EMPLOYEES_HOT",
                        _stage_data_table_name = "EMPLOYEES_HOT",
                        _actual_data_table_name = "EMPLOYEES_TRANSFER",
                        _historical_data_table_name = "EMPLOYEES_TRANSFER_HIST",
                        _delta_number = 40
                    }
                }
            }
    , { status = 200 })
    fiber.sleep(3)

    local value_schema, err = file_utils.read_file('test/unit/data/avro_schemas/topicEmployees_avro_schema_valid.json')
    t.assert_equals(err, nil)

    local producer, err2 = tnt_kafka.Producer.create({ brokers = "localhost:9092" })
    t.assert_equals(err2, nil)

    local encoded_value = { { 0, 0, "test", "test", "test", 0, 0 } }

    local i = 0
    fiber.new(function()
        while true do
            i = i + 1
            encoded_value[1][1] = i
            local _, decoded_value = bin_avro_utils.encode(
                    value_schema,
                    encoded_value,
                    true
            )

            err = producer:produce({
                topic = "EMPLOYEES",
                key = "test_key",
                value = decoded_value
            })
            t.assert_equals(err)
        end
    end)

    -- wait data in storage staging table
    local staging_rec_count_s1 = storage1:call('storage_space_count', { 'EMPLOYEES_HOT' })
    local staging_rec_count_s2 = storage2:call('storage_space_count', { 'EMPLOYEES_HOT' })

    while staging_rec_count_s1 == 0 or staging_rec_count_s2 == 0 do
        staging_rec_count_s2 = storage2:call('storage_space_count', { 'EMPLOYEES_HOT' })
        staging_rec_count_s1 = storage1:call('storage_space_count', { 'EMPLOYEES_HOT' })
    end


    assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_actual_data_table_name=EMPLOYEES_TRANSFER&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil,
            { status = 200 })

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/EMPLOYEES',
            nil,
            { status = 200 })

    -- if add pause in api query sequence, when code works correct
    --fiber.sleep(10)
    assert_http_json_request('POST',
            '/api/v1/ddl/table/reverseHistoryTransfer',
            {
                stagingTableName = "EMPLOYEES_HOT",
                actualTableName = "EMPLOYEES_TRANSFER",
                historyTableName = "EMPLOYEES_TRANSFER_HIST",
                sysCn = 1
            },
            { status = 200 })

    staging_rec_count_s1 = storage1:call('storage_space_count', { 'EMPLOYEES_HOT' })
    staging_rec_count_s2 = storage2:call('storage_space_count', { 'EMPLOYEES_HOT' })

    t.assert_equals(staging_rec_count_s1, 0)
    t.assert_equals(staging_rec_count_s2, 0)
end

