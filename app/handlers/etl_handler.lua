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
--- DateTime: 3/25/20 3:49 PM

local json = require('json')
local url_utils = require('app.utils.url_utils')

local success_repository = require('app.messages.success_repository')
local error_repository = require('app.messages.error_repository')

local function drop_space_on_cluster(req)

    local space_name = url_utils.url_decode(req:query_param('_space_name'))
    local schema_ddl_correction = url_utils.url_decode(req:query_param('_clear_schema_ddl'))

    if space_name == nil then
        return error_repository.return_http_response('DROP_SPACE_ON_CLUSTER_001')
    end

    local ok,err = _G.drop_space_on_cluster(space_name,schema_ddl_correction)

    if ok then return success_repository.return_http_response('DROP_SPACE_ON_CLUSTER_001')
    else error_repository.return_http_response('DROP_SPACE_ON_CLUSTER_002', {error = err})
    end
end

local function transfer_data_to_historical_table(req)

    local actual_data_table_name = url_utils.url_decode(req:query_param('_actual_data_table_name'))

    if actual_data_table_name == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001')
    end

    local historical_data_table_name = url_utils.url_decode(req:query_param('_historical_data_table_name'))

    if historical_data_table_name == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_002')
    end

    local delta_number = tonumber(req:query_param('_delta_number'))

    if delta_number == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_003')
    end

    --TODO Add counter?

    local ok,err = _G.transfer_data_to_historical_table_on_cluster(actual_data_table_name,
                                                                    historical_data_table_name,
                                                                    delta_number)

    if ok then return success_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001')
        else
        if type(err) == 'string' then
            return error_repository.return_http_response(json.decode(err)['errorCode'],err)
        else return error_repository.return_http_response('ROLE_VALIDATE_CONFIG_ERR_001',err)
        end
    end

end

local function transfer_data_to_scd_table(req)

    local stage_data_table_name = url_utils.url_decode(req:query_param('_stage_data_table_name'))

    if stage_data_table_name == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_SCD_TABLE_001')
    end

    local actual_data_table_name = url_utils.url_decode(req:query_param('_actual_data_table_name'))

    if actual_data_table_name == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001')
    end

    local historical_data_table_name = url_utils.url_decode(req:query_param('_historical_data_table_name'))

    if historical_data_table_name == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_002')
    end

    local delta_number = tonumber(req:query_param('_delta_number'))

    if delta_number == nil then
        return error_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_003')
    end

    --TODO Add counter?

    local ok,err = _G.transfer_data_to_scd_table_on_cluster(stage_data_table_name,
                                                            actual_data_table_name,
                                                            historical_data_table_name,
                                                            delta_number)

    if ok then return success_repository.return_http_response('API_ETL_TRANSFER_DATA_TO_HISTORICAL_TABLE_001')
    else
        if type(err) == 'string' then
            return error_repository.return_http_response(json.decode(err)['errorCode'],err)
        else return error_repository.return_http_response('ROLE_VALIDATE_CONFIG_ERR_001',err)
        end
    end

end


local function reverse_history_in_scd_table(req)
    local body = req:json()

    if body.stagingTableName == nil then
        return error_repository.return_http_response('API_ETL_REVERSE_DATA_TO_SCD_TABLE_001')
    end


    if body.actualTableName == nil then
        return error_repository.return_http_response('API_ETL_REVERSE_DATA_TO_SCD_TABLE_003')
    end


    if body.historyTableName == nil then
        return error_repository.return_http_response('API_ETL_REVERSE_DATA_TO_SCD_TABLE_005')
    end

    if body.sysCn == nil then
        return error_repository.return_http_response('API_ETL_REVERSE_DATA_TO_SCD_TABLE_007')
    end

    local _,err = _G.reverse_history_in_scd_table_on_cluster(
            body.stagingTableName,
            body.actualTableName,
            body.historyTableName,
            tonumber(body.sysCn),
            tonumber(body.eraseOperationBatchSize)
    )

    if err ~= nil then
        return error_repository.return_http_response('API_ETL_REVERSE_DATA_TO_SCD_TABLE_009', {error = err})
    end

    return {status = 200}
end

local function delete_data_from_scd_table_sql(req)
    local body = req:json()

    if body.spaceName == nil then
        return error_repository.return_http_response('API_ETL_DELETE_DATA_FROM_SCD_TABLE_001')
    end

    local _,err = _G.delete_data_from_scd_table_sql_on_cluster(body.spaceName, body.whereCondition)

    if err ~= nil then
        return error_repository.return_http_response('API_ETL_DELETE_DATA_FROM_SCD_TABLE_002', {error = err})
    end

    return {status = 200}
end

--- Get checksum for a subset of data. The function calculates a checksum within a delta for all of the
--- logical tables in the datamart or for one logical table.
--- This method implements function CHECK_SUM from DTM SQL (https://arenadata.atlassian.net/wiki/spaces/DTM/pages/475856930/SQL+CHECK+SUM)
--- @param req table - body of request: Example json body:
---                    {
---                         "actualDataTableName": "test",
---                         "historicalDataTableName": "test",
---                         "sysCn": 0,
---                         "columnList": null,
---                         "normalization": null
---                     }
local function get_scd_table_checksum (req)
    local body = req:json()
    if body.actualDataTableName == nil then
        return error_repository.return_http_response('API_ETL_GET_SCD_CHECKSUM_001')
    end

    if body.historicalDataTableName == nil then
        return error_repository.return_http_response('API_ETL_GET_SCD_CHECKSUM_002')
    end

    if body.sysCn == nil then
        return error_repository.return_http_response('API_ETL_GET_SCD_CHECKSUM_003')
    end

    local norm = body.normalization
    if norm ~= nil then
        if norm < 1 then
            return error_repository.return_http_response('API_ETL_GET_SCD_CHECKSUM_004')
        end
        norm = 1
    end

    local is_ok,res = _G.get_scd_table_checksum_on_cluster(body.actualDataTableName, body.historicalDataTableName, 
                                                           body.sysCn, body.columnList, norm)

    if not is_ok then
        return error_repository.return_http_response('API_ETL_GET_SCD_CHECKSUM_005', {error = res})
    end

    return {status = 200, body = json.encode({checksum = res})}
end

return {
    transfer_data_to_historical_table = transfer_data_to_historical_table,
    transfer_data_to_scd_table = transfer_data_to_scd_table,
    drop_space_on_cluster = drop_space_on_cluster,
    reverse_history_in_scd_table = reverse_history_in_scd_table,
    delete_data_from_scd_table_sql = delete_data_from_scd_table_sql,
    get_scd_table_checksum = get_scd_table_checksum
}