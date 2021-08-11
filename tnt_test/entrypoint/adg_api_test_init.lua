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


require('strict').on()
local schema_utils = require('app.utils.schema_utils')
local json = require('json')
local log = require('log')
local errors = require('errors')
local err_storage = errors.new_class("Storage error")
local prepared_statements = {}

local log_queries = false


_G.transfer_stage_data_to_scd_table = nil
_G.reverse_history_in_scd_table = nil
_G.insert_tuples = nil
_G.set_schema_ddl = nil
_G.execute_sql = nil
package.preload['fake_adg_storage'] = function()

    return {
        role_name = 'app.roles.adg_storage',
        init = function()
            local checks = require('checks')
            local fiber = require('fiber')

            local function transfer_data_to_historical_table(actual_data_table_name,historical_data_table_name, delta_number)
                checks('string', 'string', 'number')

                fiber.self():cancel()
            end

            local function transfer_stage_data_to_scd_table(stage_data_table_name, actual_data_table_name,historical_data_table_name, delta_number)
                checks('string','string','string','number')

                fiber.self():cancel()
            end

            local function reverse_history_in_scd_table(stage_data_table_name, actual_data_table_name,historical_data_table_name, delta_number,batch_size)
                checks('string','string','string','number','?number')

                fiber.self():cancel()
            end

            local function insert_tuples(tuples,is_record_type)
                checks('table','?boolean')

                fiber.self():cancel()
            end

            local function set_schema_ddl()
                schema_utils.init_schema_ddl()
                return schema_utils.set_schema(schema_utils.get_schema_ddl())
            end

            local function execute_sql(query,params)
                checks('string', '?table')

                local res, err = nil, nil

                if log_queries then
                    log.warn("query: %s, params: %s", query, json.encode(params))
                end

                local prep = prepared_statements[query]
                if prep ~= nil then

                    if params == nil then
                        res,err =  err_storage:pcall(prep.execute,prep)
                    else
                        res,err =  err_storage:pcall(prep.execute,prep,params)
                    end

                else
                    if params == nil then
                        res,err = err_storage:pcall(box.execute, query)
                    else
                        res,err = err_storage:pcall(box.execute, query, params)
                    end
                end

                return res,err
            end

            _G.transfer_data_to_historical_table = transfer_data_to_historical_table
            _G.transfer_stage_data_to_scd_table = transfer_stage_data_to_scd_table
            _G.reverse_history_in_scd_table = reverse_history_in_scd_table
            _G.insert_tuples = insert_tuples
            _G.set_schema_ddl = set_schema_ddl
            _G.execute_sql = execute_sql
        end,
        dependencies = {'cartridge.roles.vshard-storage'}
    }
end

_G.send_query_to_kafka_with_plan = nil
package.preload['fake_adg_output_processor'] = function()
    return {
        role_name = 'app.roles.adg_output_processor',
        init = function()
            local fiber = require('fiber')

            local function send_query_to_kafka_with_plan(replica_uuid,plan,stream_number,stream_total,params)
                fiber.self():cancel()
            end

            _G.send_query_to_kafka_with_plan = send_query_to_kafka_with_plan
        end,
        dependencies = {'cartridge.roles.vshard-router'}
    }
end

package.setsearchroot('.')

local cartridge = require('cartridge')
local ok, err = cartridge.cfg({
    workdir = 'tmp/db',
    roles = {
        'cartridge.roles.vshard-storage',
        'cartridge.roles.vshard-router',
        'app.roles.adg_api',
        'fake_adg_storage',
        'app.roles.adg_kafka_connector',
        'app.roles.adg_scheduler',
        'app.roles.adg_input_processor',
        'fake_adg_output_processor',
        'app.roles.adg_state'
    },
    cluster_cookie = 'memstorage-cluster-cookie',
})


assert(ok, tostring(err))
