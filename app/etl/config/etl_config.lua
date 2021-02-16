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
--- DateTime: 4/7/20 4:57 PM
---
local config_utils = require('app.utils.config_utils')

local function get_etl_scd_opts(conf)
    if conf == nil then
        return {}
    end

    if conf['etl_scd'] == nil then
        return {}
    end

    return conf['etl_scd']
end

local etl_scd_opts = get_etl_scd_opts(config_utils.get_config())

local function init_etl_opts()
    etl_scd_opts = get_etl_scd_opts(config_utils.get_config())
end


local function get_date_field_start_nm()
    return etl_scd_opts.start_field_nm or 'sys_from'
end

local function get_date_field_end_nm()
    return etl_scd_opts.end_field_nm or 'sys_to'
end

local function get_date_field_op_nm()
    return etl_scd_opts.op_field_nm or 'sys_op'
end

local function get_date_field_start_index_nm()
    return etl_scd_opts.start_field_index_nm or "x_sys_from"
end

local function get_date_field_end_index_nm()
    return etl_scd_opts.end_field_index_nm or "x_sys_to"
end

local function get_transfer_pause_rows_cnt()
    return etl_scd_opts.transfer_pause_rows_cnt or 100
end

return {
    init_etl_opts = init_etl_opts,
    get_date_field_start_nm = get_date_field_start_nm,
    get_date_field_end_nm = get_date_field_end_nm,
    get_date_field_op_nm = get_date_field_op_nm,
    get_date_field_start_index_nm = get_date_field_start_index_nm,
    get_date_field_end_index_nm = get_date_field_end_index_nm,
    get_transfer_pause_rows_cnt = get_transfer_pause_rows_cnt
}