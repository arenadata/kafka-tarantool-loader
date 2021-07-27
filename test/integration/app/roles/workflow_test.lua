local t = require('luatest')
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

g.test_subscribe_unsubscribe_flow = function()
    local storage1 = cluster:server('master-1-1').net_box
    local storage2 = cluster:server('master-2-1').net_box

    assert_http_json_request('POST',
            '/api/v1/kafka/subscription', {
                topicName = "EMPLOYEES",
                spaceNames = { "EMPLOYEES_HOT" },
                avroSchema = { type = "long" },
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

    -- TODO: вставить загрузку данных в kafka

    local staging_rec_count_s1 = storage1:call('storage_space_count', { 'EMPLOYEES_HOT' })
    local staging_rec_count_s2 = storage2:call('storage_space_count', { 'EMPLOYEES_HOT' })

    t.assert_not_equals(staging_rec_count_s1, 0)
    t.assert_not_equals(staging_rec_count_s2, 0)

    assert_http_json_request('GET',
            '/api/etl/transfer_data_to_scd_table?_stage_data_table_name=EMPLOYEES_HOT&_actual_data_table_name=EMPLOYEES_TRANSFER&_historical_data_table_name=EMPLOYEES_TRANSFER_HIST&_delta_number=2',
            nil,
            { status = 200 })

    assert_http_json_request('DELETE',
            '/api/v1/kafka/subscription/EMPLOYEES',
            nil,
            { status = 200 })

    assert_http_json_request('DELETE',
            '/api/v1/ddl/table/reverseHistoryTransfer',
            {
                stagingTableName = "EMPLOYEES_HOT",
                actualTableName = "EMPLOYEES_TRANSFER",
                historyTableName = "EMPLOYEES_TRANSFER_HIST",
                sysCn = 1
            },
            { status = 200 })

    t.assert_equals(staging_rec_count_s1, 0)
    t.assert_equals(staging_rec_count_s2, 0)
end

