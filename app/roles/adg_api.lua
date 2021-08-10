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
local checks = require('checks')
local schema_utils = require('app.utils.schema_utils')
local route_utils = require('app.utils.route_utils')
local sql_insert = require('app.utils.sql_insert')
local yaml = require('yaml')
local log = require('log')
local errors = require('errors')
local vshard = require('vshard')
local sql_select = require('app.utils.sql_select')
local fiber = require('fiber')
local pool = require('cartridge.pool')
local metrics = require('app.metrics.metrics_storage')
local set = require('app.entities.set')
local misc_utils = require('app.utils.misc_utils')
local prometheus = require('metrics.plugins.prometheus')
local api_timeout_config = require('app.utils.api_timeout_config')
-- local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local role_name = 'app.roles.adg_api'
-- local json = require('json')

local garbage_fiber = nil

local cluster_config_handler = require('app.handlers.cluster_config_handler')
local select_query_to_kafka_handler = require('app.handlers.select_table_to_kafka_handler')
local get_all_metrics_handler = require('app.handlers.get_all_metrics_handler')
local etl_handler = require('app.handlers.etl_handler')
local truncate_space_handler = require('app.handlers.truncate_space_handler')
local kafka_handler = require('app.handlers.kafka_handler')
local ddl_handler = require('app.handlers.ddl_handler')
local version_handler = require('app.handlers.version_handler')

local success_repository = require('app.messages.success_repository')
local error_repository = require('app.messages.error_repository')

local fun = require('fun')

_G.set_ddl = nil
_G.get_ddl = nil
_G.query = nil
_G.drop_space_on_cluster = nil
_G.truncate_space_on_cluster = nil
_G.drop_all = nil
_G.space_len = nil
_G.load_lines = nil
_G.consume_all = nil
_G.get_metric = nil
_G.sync_ddl_schema_with_storage = nil

local function transfer_data_to_historical_table_on_cluster(actual_data_table_name,historical_data_table_name, delta_number)
    checks('string', 'string', 'number')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true}) --TODO Move to single function
    if #storages == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_storage'})
    end

    local futures = {}
    for _, cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return nil, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, err = conn:call(
                'transfer_data_to_historical_table',
                { actual_data_table_name, historical_data_table_name, delta_number },
                { is_async = true }
            )
            if err ~= nil then
                return nil, err
            end
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(_G.api_timeouts:get_transfer_stage_data_to_scd_table_timeout())
        local res, err = future:result()
        if res == nil then
            return nil, error_repository.get_error_code(
                'STORAGE_003', {
                    func='transfer_data_to_historical_table',
                    actual_data_table_name = actual_data_table_name,
                    historical_data_table_name = historical_data_table_name,
                    delta_number = delta_number,
                    error = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return nil, res[2]
        end
    end

    return true, nil
end

_G.transfer_data_to_historical_table_on_cluster = nil
_G.execute_query_for_massive_select_to_kafka = nil
_G.transfer_data_to_scd_table_on_cluster = nil
_G.drop_spaces_on_cluster = nil
_G.transfer_data_to_scd_table_on_cluster_cb = nil
_G.reverse_history_in_scd_table_on_cluster = nil
_G.get_storage_space_schema = nil
_G.delete_data_from_scd_table_sql_on_cluster = nil
_G.get_scd_table_checksum_on_cluster = nil
_G.api_timeouts = nil

local err_vshard_router = errors.new_class("Vshard routing error")


local function set_ddl(ddl)
    checks('string')

    local res, err = _G.cartridge_set_schema(ddl)


    if res == nil then
        return nil, err
    end

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})

    for _,storage in ipairs(storages) do

        local conn, err = pool.connect(storage)
        if conn == nil then
            log.error(err)
            return nil, err
        end

        local res,err = conn:call('set_schema_ddl',{},{is_async=false})

        if res == nil then
            log.error(err)
            return nil,err
        end
    end


    return true
end



---sync_ddl_schema_with_storage
---@param storage string
local function sync_ddl_schema_with_storage(storage)
    checks('string')

    local storages =  set.Set(cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true}))

    if storages[storage] then

        local conn, err = pool.connect(storage)
        if conn == nil then
            log.error(err)
            return nil, err
        end

        local schema,err = conn:call('get_storage_ddl',{},{is_async=false})

        if schema == nil then
            log.error(err)
            return nil,err
        end

        local res,err = err_vshard_router:pcall(_G.cartridge_set_schema, yaml.encode(schema))
        if res == nil then
            log.error(err)
            return nil, err
        end

        return true, nil
    end

    return false,storage .. ' dont have app.roles.adg_storage role'
end

---drop_space_on_cluster - function to drop space on cluster.
---@param space_name string  - space name to drop.
---@param schema_ddl_correction boolean - flag, that shows update ddl schema or not. Default - True.
---@return boolean | table - true,nil if dropped | false,error - otherwise.
local function drop_space_on_cluster(space_name,schema_ddl_correction)
    checks('string','?boolean')
    if schema_ddl_correction == nil then
        schema_ddl_correction = true
    end
    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})

    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false,err
        else
            local is_space_dropped,space_drop_err  = conn:call('drop_space',{space_name},{timeout = 30, is_async=false})

            if is_space_dropped ~= true then
                return false,space_drop_err
            end
        end
        fiber.sleep(0.01) -- Wait async replicas for processing changes.
    end

    if schema_ddl_correction == true then
        local current_ddl_schema = yaml.decode(cartridge.get_schema())
        current_ddl_schema.spaces[space_name] = nil
        local is_ddl_schema_patched, schema_patch_err =cartridge.set_schema(yaml.encode(current_ddl_schema))

        if is_ddl_schema_patched == nil then
            return false, schema_patch_err
        end
    end
    return true, nil
end

---truncate_space_on_cluster - function to truncate space on cluster.
---@param space_name string  - space name to truncate.
---@return boolean | table - true,nil if dropped | false,error - otherwise.
local function truncate_space_on_cluster(space_name)
    checks('string')
    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})

    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false,err
        else
            local is_space_truncated, space_truncate_err  = conn:call(
                'truncate_space',
                { space_name },
                { timeout = 30, is_async=false }
            )

            if is_space_truncated ~= true then
                return false, space_truncate_err
            end
        end
        fiber.sleep(0.01) -- Wait async replicas to process changes
    end

    return true, nil
end

---drop_spaces_on_cluster - function to drop spaces on cluster.
---@param spaces string  - list of spaces to drop.
---@param schema_ddl_correction boolean - flag, that shows update ddl schema or not. Defualt - True.
---@return table | string - dropped spaces,nil | dropped_spaces,error.
local function drop_spaces_on_cluster(spaces,prefix,schema_ddl_correction)
    checks('?table','?string','?boolean')

    if schema_ddl_correction == nil then
        schema_ddl_correction = true
    end
    local dropped_spaces,spaces_drop_err
    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})

    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return nil,err
        else

            if spaces ~= nil then
                dropped_spaces,spaces_drop_err  = conn:call('drop_spaces',{spaces},{timeout = 30, is_async=false})
            elseif prefix ~= nil then
                dropped_spaces,spaces_drop_err  = conn:call('drop_spaces_with_prefix',{prefix},{timeout = 30, is_async=false})
            else
                return nil,'ERROR: spaces and prefix is nil'
            end

            if spaces_drop_err ~= nil then
                return dropped_spaces,spaces_drop_err
            end

        end
        fiber.sleep(0.01) -- Wait async replicas for processing changes.
    end


    if schema_ddl_correction == true and not (dropped_spaces == nil or next(dropped_spaces) ==  nil) then
        local current_ddl_schema = yaml.decode(cartridge.get_schema())
        for _,v in ipairs(dropped_spaces) do
            current_ddl_schema.spaces[v] = nil
        end
        local tries_cnt = 0
        ::retry::
        local is_ddl_schema_patched, schema_patch_err = cartridge.set_schema(yaml.encode(current_ddl_schema))
        if is_ddl_schema_patched == nil then
            tries_cnt = tries_cnt + 1
            if tries_cnt >= 20 then
                return dropped_spaces, schema_patch_err
            else
                log.error(schema_patch_err)
                fiber.sleep(0.5)
                goto retry
            end
        end
    end
    --ddl_handler.queued_tables_notify(dropped_spaces)
    return dropped_spaces, nil
end

local function drop_all()
    local replicas, _ = vshard.router.routeall()

--    local result = nil
    for _, replica in pairs(replicas) do
        local res, err = replica:callro("storage_drop_all")

        if res == nil then
            return nil, err
        end
    end

    local res, err = cartridge.config_patch_clusterwide({schema=""})

    if res == nil then
        return nil, err
    end

    return true
end

local function space_len(space_name)
    checks('string')
    local replicas, _ = vshard.router.routeall()

    local result = 0
    for _, replica in pairs(replicas) do
        local res, err = replica:callro("storage_space_len", {space_name})

        if res == nil then
            return nil, err
        end

        result = result + res
    end

    return result
end

local function load_lines(space_name, lines)
    checks("string", "table")


    local space = schema_utils.get_schema_ddl().spaces[space_name]
     or schema_utils.get_schema_ddl().spaces[string.upper(space_name)]


    if space == nil then
        return nil, errors.new("ERROR: no_such_space", "No such space: %s", space_name)
    end

    local tuples = {}

    for _, line in ipairs(lines) do
        local err
        line, err = schema_utils.from_csv_line(space_name, line)

        if line == nil then
            return nil, err
        end

        local tuple, err_bucket = route_utils.set_bucket_id(space_name, line, vshard.router.bucket_count())


        if tuple == nil then
            return nil, err_bucket
        end
        table.insert(tuples, tuple)
    end


    local futures = {}


    for server, per_server in pairs(route_utils.tuples_by_server(tuples, space_name, vshard.router.bucket_count())) do
        local future = server:call(
            'insert_tuples',
            {{[space_name]=per_server}},
            {is_async=true}
        )
        table.insert(futures,future)
    end

    for _, future in ipairs(futures) do
        future:wait_result()
        local res, err = future:result()
        if res == nil then
            return nil, err
        end
    end

    return true
end

--- store procedure is called by Prostore, then it runs SELECT query.
--- When this procedure received sql query it sends this query to all storage instances.
local function query(query, params)
    local lower_query = string.lower(query)

    if string.match(lower_query, "^%s*insert%s+") then
        local sql_res, err = sql_insert.get_tuples(
            query, params,vshard.router.bucket_count())

        if sql_res == nil then
            return nil, err
        end

        local by_bucket_id = sql_insert.tuples_by_bucket_id(
            sql_res.inserted_tuples, vshard.router.bucket_count())


        for bucket_id, tuples in pairs(by_bucket_id) do
            local res, err = err_vshard_router:pcall(
                vshard.router.call,
                bucket_id,
                'write',
                'insert_tuples',
                {tuples}
            )

            if res == nil then
                return nil, err
            end
        end

        return sql_res.sql_result

    elseif string.match(lower_query, "^%s*insert%s+into+%s+[%a%d_]+[(]*%a*[)]*%s+select+") then

        local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})
        if #storages == 0 then
            log.error('ERROR: storage to execute ddl not found')
        end

        for _,cand in ipairs(storages) do
            log.info('INFO: Storage for ddl execution ' .. cand)

            local conn, err = pool.connect(cand)
            if conn == nil then
                log.error(err)
            else
            local res,err = conn:call('execute_sql',{query, params},{is_async=false})

            if res == nil or res == false then
                return nil, err
            end

            end
        end

        return true

    elseif string.match(lower_query, "^%s*select%s+") then
        local replicas, err = sql_select.get_replicas(
            query, params)

        if replicas == nil then
            return nil, err
        end

        local result = nil
        for _, replica in pairs(replicas) do
            local res, err = replica:callro("execute_sql",
                                            { query, params })

            if res == nil then
                return nil, err
            end

            if result == nil then
                result = res
            else
                result.rows = misc_utils.append_table(result.rows, res.rows)
            end
        end

        return result

    elseif string.match(lower_query, "^%s*create%s+") or string.match(lower_query, "^%s*alter%s+")
           or string.match(lower_query, "^%s*drop%s+") or string.match(lower_query, "^%s*truncate%s+") then

            local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})
            if #storages == 0 then
                log.error('ERROR: storage to execute ddl not found')
            end

            for _,cand in ipairs(storages) do
                log.info('INFO: Storage for ddl execution ' .. cand)

                local conn, err = pool.connect(cand)
                if conn == nil then
                    log.error(err)
                else
                local res,err = conn:call('execute_sql',{query, params},{is_async=false})

                if res == nil or res == false then
                    return nil, err
                end

                end
            end

        return true

    else
        return nil, errors.new('Unknown query type')
    end

    return true -- luacheck: ignore 511
end

local function get_metric()
    return metrics.export(role_name)
end

---transfer_data_to_historical_table_on_cluster
---Method, that performs transfer between actual and historical tables on all master storages in cluster.
---Example of usage: transfer_data_to_historical_table_on_cluster('EMPLOYEES', 'EMPLOYEES_HIST',2)
---@param actual_data_table_name string - Name of the table, that contains actual data for delta processing.
---@param historical_data_table_name string - Name of the table, that contains historical data for delta processing.
---@param delta_number number - Number, that marks new version of data.
local function transfer_data_to_historical_table_on_cluster(actual_data_table_name,historical_data_table_name, delta_number)
    checks('string', 'string', 'number')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true}) --TODO Move to single function
    if #storages == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_storage'})
    end

    local futures = {}
    for _, cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return nil, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, err = conn:call(
                'transfer_data_to_historical_table',
                { actual_data_table_name, historical_data_table_name, delta_number },
                { is_async = true }
            )
            if err ~= nil then
                return nil, err
            end
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(_G.api_timeouts:get_transfer_stage_data_to_scd_table_timeout())
        local res, err = future:result()
        if res == nil then
            return nil, error_repository.get_error_code(
                'STORAGE_003', {
                    func='transfer_data_to_historical_table',
                    actual_data_table_name = actual_data_table_name,
                    historical_data_table_name = historical_data_table_name,
                    delta_number = delta_number,
                    error = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return nil, res[2]
        end
    end

    return true, nil
end

local function transfer_data_to_scd_table_on_cluster(stage_data_table_name,actual_data_table_name,historical_data_table_name, delta_number)
    checks('string', 'string','string', 'number')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true}) --TODO Move to single function
    if #storages == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_storage'})
    end

    local futures = {}
    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return nil, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, err = conn:call(
                'transfer_stage_data_to_scd_table',
                { stage_data_table_name,actual_data_table_name, historical_data_table_name, delta_number },
                { is_async = true }
            )
            if err ~= nil then
                return nil, err
            end
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(_G.api_timeouts:get_transfer_stage_data_to_scd_table_timeout())
        local res, err = future:result()
        if res == nil then
            return nil, error_repository.get_error_code(
                'STORAGE_003', {
                    func='transfer_stage_data_to_scd_table',
                    stage_data_table_name = stage_data_table_name,
                    actual_data_table_name = actual_data_table_name,
                    historical_data_table_name = historical_data_table_name,
                    delta_number = delta_number,
                    error = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return nil, res[2]
        end
    end

    return true, nil
end

local function reverse_history_in_scd_table_on_cluster(stage_data_table_name, actual_data_table_name,
                                                       historical_data_table_name, delta_number, batch_size)
    checks('string','string','string','number','?number')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true}) --TODO Move to single function
    if #storages == 0 then
        return false, error_repository.get_error_code(
            'VROUTER_REPLICA_GET_001', {
                role = 'app.roles.adg_storage'
            }
        )
    end

    local futures = {}
    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, err = conn:call(
                'reverse_history_in_scd_table',
                { stage_data_table_name,actual_data_table_name, historical_data_table_name, delta_number, batch_size },
                { is_async = true }
            )
            if err ~= nil then
                return nil, err
            end
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(360)
        local res, err = future:result()
        if res == nil then
            return nil, error_repository.get_error_code(
                'STORAGE_003', {
                    func='reverse_history_in_scd_table',
                    stage_data_table_name = stage_data_table_name,
                    actual_data_table_name = actual_data_table_name,
                    historical_data_table_name = historical_data_table_name,
                    delta_number = delta_number,
                    batch_size = batch_size,
                    error = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return nil, res[2]
        end
    end

    return true, nil
end


local function transfer_data_to_scd_table_on_cluster_cb(params)
    checks('table')


        local space_name = params['_space']
        local stage_data_table_name = params['_stage_data_table_name']
        local actual_data_table_name = params['_actual_data_table_name']
        local historical_data_table_name = params['_historical_data_table_name']
        local delta_number = params['_delta_number']
        local res,err = transfer_data_to_scd_table_on_cluster(stage_data_table_name, actual_data_table_name,
                                                              historical_data_table_name, delta_number)

        if res ~= true then
            log.error(err)
            return res,err
        end
        log.info('INFO: %s space successful processed', space_name)


    return true,nil
end

---prepare_plan_for_massive_select - method that generate split for query by batch_size.
---@param query string - select query string to execute on storage nodes.
---@param batch_size number - a number of rows in one subquery (limit, offset).
---@return table - boolean | {[storage] = {limit = ?, offset = ?} table.
local function prepare_plan_for_massive_select(query, batch_size)
    checks('string','number')

    if not string.match(string.lower(query), "^%s*select%s+") then
        return false,error_repository.get_error_code('ADG_OUTPUT_PROCESSOR_003',{query=query})
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

    return true, split_query
end

---prepare_output_processors_for_plan - method, that's assign output processor for each task.
---@param plan table - {[storage] = {limit = ?, offset = ?} table.
---@return table - boolean | {[output_processor] = {[storage] = {limit = ?, offset = ?}} table.
local function prepare_output_processors_for_plan(plan)
    checks('table')
    local output_processors = cartridge.rpc_get_candidates('app.roles.adg_output_processor')

    if #output_processors == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_output_processor'})
    end
    return true, fun.zip(plan,fun.cycle(output_processors)):tomap()
end

---execute_query_for_massive_select - method, that's execute processor_function
---with params opts on output processor role.
---@param processor_function string - function name to run on output processor.
---@param params table - table with params needed by processor_function.
---@return boolean|string
local function execute_query_for_massive_select(processor_function, params)
    checks('string', 'table')
    local batch_size = params['batch_size'] or 1000
    local select_query = params['query'] or ''

    local is_plan_ok, plan = prepare_plan_for_massive_select(select_query,batch_size)

    if not is_plan_ok then
        return false, plan
    end

    local is_plan_mapping_ok, plan_mapping = prepare_output_processors_for_plan(plan)

    if not is_plan_mapping_ok then
        return false, plan_mapping
    end

    local futures = {}

    local stream_number = 0
    local stream_total = misc_utils.table_length(plan_mapping)
    for replica, output_processor in pairs(plan_mapping) do
        stream_number = stream_number + 1
        local conn, err = pool.connect(output_processor,{wait_connected = 60})
        if conn == nil then
            return nil, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_output_processor',
                    error = err
                }
            )
        end


        local future, err = conn:call(
            processor_function,
            { replica.uuid, plan[replica], stream_number, stream_total, params },
            { is_async = true }
        )
        if err ~= nil then
            return nil, err
        end
        table.insert(futures,future)
    end

    for _,future in ipairs(futures) do
        future:wait_result(360)
        local res, err = future:result()

        if res == nil then
            return nil, error_repository.get_error_code(
                'ADG_OUTPUT_PROCESSOR_001', {
                    error = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return nil, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_output_processor',
                    error = err or res[2]
                }
            )
        end
    end


    return true, success_repository.get_success_code('ADG_OUTPUT_PROCESSOR_002')

end

local function execute_query_for_massive_select_to_kafka(topic_name, query, batch_size,table_name,avro_schema)
    return execute_query_for_massive_select('send_query_to_kafka_with_plan',
            {topic_name = topic_name, query = query,batch_size=batch_size,table_name = table_name,avro_schema=avro_schema})

end

local function get_storage_space_schema(space_names)
    checks('table')
    local space_names_set = set.Set(space_names)
-- luacheck: ignore v
    return yaml.encode({spaces = fun.filter(function(k,v) return space_names_set[k] end, schema_utils.get_schema_ddl().spaces):tomap()})
end

local function delete_data_from_scd_table_sql_on_cluster (space_name, where_condition)
    checks('string', '?string')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})
    if #storages == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_storage'})
    end

    local futures = {}
    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, future_err = conn:call(
                'delete_data_from_scd_table_sql',
                {space_name, where_condition},
                { is_async = true }
            )
            if future_err ~= nil then
                return false, future_err
            end
            table.insert(futures,future)
        end
    end

    for _,future in ipairs(futures) do
        future:wait_result(360)
        local res, err = future:result()
        if res == nil then
            return false, error_repository.get_error_code(
                'STORAGE_003', {
                    func='delete_data_from_scd_table_sql',
                    space_name = space_name,
                    where_condition = where_condition,
                    err = err
                }
            )
        end

        if res[1] == false or res[1] == nil or res[2] ~= nil then
            return false, res[2]
        end
    end

    return true, nil

end

--- Get checksum for a subset of data on cluster
--- @param actual_data_table_name string - space for actual table
--- @param historical_data_table_name string - space for history table
--- @param delta_number number - delta (https://arenadata.atlassian.net/wiki/spaces/DTM/pages/46653935/delta)
--- @param column_list table - optional, columns list for calculate checksum
--- @param normalization number - optional, coefficient of increasing the possible number
--                                of records within the delta. (positive integer greater than or equal to 1, default 1).
local function get_scd_table_checksum_on_cluster(actual_data_table_name, historical_data_table_name,
                                                 delta_number, column_list, normalization)
    checks('string','string','number','?table','?number')

    local storages =  cartridge.rpc_get_candidates('app.roles.adg_storage',{leader_only = true})
    if #storages == 0 then
        return false, error_repository.get_error_code('VROUTER_REPLICA_GET_001', {role='app.roles.adg_storage'})
    end

    local futures = {}
    for _,cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false, error_repository.get_error_code(
                'VROUTER_REPLICA_GET_001', {
                    role = 'app.roles.adg_storage',
                    error = err
                }
            )
        else
            local future, future_err = conn:call(
                'get_scd_table_checksum',
                {actual_data_table_name, historical_data_table_name, delta_number, column_list, normalization},
                { is_async = true }
            )
            if future_err ~= nil then
                return false, future_err
            end
            table.insert(futures,future)
        end
    end

    local result = 0
    for _,future in ipairs(futures) do
        future:wait_result(_G.api_timeouts:get_scd_table_checksum_timeout())
        local res, err = future:result()
        if res == nil then
            return false, error_repository.get_error_code(
                'STORAGE_003', {
                    func='delete_data_from_scd_table_sql',
                    actual_data_table_name = actual_data_table_name,
                    historical_data_table_name = historical_data_table_name,
                    delta_number = delta_number,
                    column_list = column_list,
                    err = err
                }
            )
        end

        if res[1] == false or res[1] == nil then
            return false, res[2]
        end

        result = result + res[2]
    end

    return true, result
end

local function init_kafka_routes()
    local httpd = cartridge.service_get('httpd')

    httpd:route({method='POST', path = 'api/v1/kafka/subscription'}, kafka_handler.subscribe_to_topic_on_cluster)
    httpd:route({method='DELETE', path = 'api/v1/kafka/subscription/:topicName'}, kafka_handler.unsubscribe_from_topic_on_cluster)
    httpd:route({method='POST', path = 'api/v1/kafka/dataload'}, kafka_handler.dataload_from_topic_on_cluster)


    httpd:route({method='POST', path = 'api/v1/kafka/dataunload/query'}, kafka_handler.dataunload_query_to_topic_on_cluster)
    httpd:route({method='POST', path = 'api/v1/kafka/dataunload/table'}, kafka_handler.dataunload_table_to_topic_on_cluster)

    httpd:route({method='POST', path = 'api/v1/kafka/callback'}, kafka_handler.register_kafka_callback_function)
    httpd:route({method='GET', path = 'api/v1/kafka/callbacks'}, kafka_handler.get_kafka_callback_functions)
    httpd:route({method='DELETE', path = 'api/v1/kafka/callback/:callbackFunctionName'},kafka_handler.delete_kafka_callback_function)

end

local function init_ddl_routes()
    local httpd = cartridge.service_get('httpd')
    httpd:route({method='DELETE', path = 'api/v1/ddl/table/:tableName'}, ddl_handler.add_table_to_delete_batch)
    httpd:route({method='PUT', path = 'api/v1/ddl/table/batchDelete'}, ddl_handler.put_tables_to_delete_batch)
    httpd:route({method='DELETE', path = 'api/v1/ddl/table/queuedDelete'}, ddl_handler.queued_tables_delete)
    httpd:route({method='POST', path = 'api/v1/ddl/table/queuedCreate'}, ddl_handler.queued_tables_create)
    httpd:route({method='DELETE', path = 'api/v1/ddl/table/queuedDelete/prefix/:tablePrefix'}, ddl_handler.queued_prefix_delete)
    httpd:route({method='DELETE', path = 'api/v1/ddl/table/batchDelete/:batchId'}, ddl_handler.delete_table_batch)
    httpd:route({method='DELETE', path = 'api/v1/ddl/table/batchDelete/prefix/:tablePrefix'}, ddl_handler.delete_table_prefix)
    httpd:route({method='POST', path = 'api/v1/ddl/table/schema'}, ddl_handler.get_storage_space_schema)
end

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.adg_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.adg_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

local function init(opts) -- luacheck: no unused args
    rawset(_G, 'ddl', { get_schema = get_schema })

    _G.set_ddl = set_ddl
    _G.get_ddl = get_ddl
    _G.query = query
    _G.drop_space_on_cluster = drop_space_on_cluster
    _G.truncate_space_on_cluster = truncate_space_on_cluster
    _G.drop_all = drop_all
    _G.space_len = space_len
    _G.get_metric = get_metric
    _G.load_lines = load_lines
    _G.sync_ddl_schema_with_storage = sync_ddl_schema_with_storage
    _G.transfer_data_to_historical_table_on_cluster = transfer_data_to_historical_table_on_cluster
    _G.transfer_data_to_scd_table_on_cluster = transfer_data_to_scd_table_on_cluster
    _G.drop_spaces_on_cluster = drop_spaces_on_cluster
    _G.execute_query_for_massive_select_to_kafka = execute_query_for_massive_select_to_kafka
    _G.transfer_data_to_scd_table_on_cluster_cb = transfer_data_to_scd_table_on_cluster_cb
    _G.reverse_history_in_scd_table_on_cluster = reverse_history_in_scd_table_on_cluster
    _G.get_storage_space_schema = get_storage_space_schema
    _G.delete_data_from_scd_table_sql_on_cluster = delete_data_from_scd_table_sql_on_cluster
    _G.get_scd_table_checksum_on_cluster = get_scd_table_checksum_on_cluster
    _G.api_timeouts = api_timeout_config.get_api_timeout_opts()

    garbage_fiber = fiber.create(
        function() while true do collectgarbage('step', 20);
            fiber.sleep(0.2) end end
    )
    garbage_fiber:name('GARBAGE_COLLECTOR_FIBER')

    local httpd = cartridge.service_get('httpd')

    httpd:route({method='GET', path = '/metrics'}, prometheus.collect_http)

    httpd:route({method='GET', path = '/api/get_config'} ,
    cluster_config_handler.cluster_config_handler)

    httpd:route({method='GET', path = '/api/kafka/send_table/:table/'},
    select_query_to_kafka_handler.select_table_to_kafka_handler)

    httpd:route({method='GET', path = '/api/kafka/send_query/'},
    select_query_to_kafka_handler.select_query_to_kafka_handler)

    httpd:route({method='GET',path = 'api/metrics/get_all_metrics'}, get_all_metrics_handler.get_all_metrics)

    httpd:route({method='GET', path = 'api/etl/transfer_data_to_scd_table'}, etl_handler.transfer_data_to_scd_table)

    httpd:route({method='POST', path = '/api/v1/ddl/table/reverseHistoryTransfer'}, etl_handler.reverse_history_in_scd_table)

    httpd:route({method='GET', path = 'api/etl/drop_space_on_cluster'}, etl_handler.drop_space_on_cluster)

    httpd:route({method='GET', path = 'api/etl/truncate_space_on_cluster'}, truncate_space_handler.truncate_space_on_cluster)

    httpd:route({method='POST', path = 'api/etl/delete_data_from_scd_table'}, etl_handler.delete_data_from_scd_table_sql)

    httpd:route({method='POST', path = 'api/etl/get_scd_table_checksum'}, etl_handler.get_scd_table_checksum)

    httpd:route({method='GET', path = 'versions'}, version_handler.get_version)

    init_kafka_routes()
    init_ddl_routes()

    return true
end

local function stop()
    garbage_fiber:cancel()
    _G.api_timeouts:clear()
    return true
end

local function validate_config(conf_new, conf_old) -- luacheck: no unused args
    if type(box.cfg) ~= 'function' and not box.cfg.read_only then -- luacheck: ignore 542
    end
    return true
end

local function apply_config(conf, opts) -- luacheck: no unused args
    _G.api_timeouts = api_timeout_config.get_api_timeout_opts()

    schema_utils.init_schema_ddl()
    error_repository.init_error_repo('en')
    success_repository.init_success_repo('en')
    if opts.is_master and pcall(vshard.storage.info) == false then
        schema_utils.drop_all()
        if conf.schema ~= nil then
            sql_select.clear_cache()
            sql_insert.install_triggers()
        end
    end
    return true
end



return {
    role_name = role_name,
    init = init,
    stop = stop,
    drop_space_on_cluster = drop_space_on_cluster,
    truncate_space_on_cluster = truncate_space_on_cluster,
    drop_spaces_on_cluster = drop_spaces_on_cluster,
    validate_config = validate_config,
    apply_config = apply_config,
    get_metric = get_metric,
    get_schema = get_schema,
    transfer_data_to_historical_table_on_cluster = transfer_data_to_historical_table_on_cluster,
    execute_query_for_massive_select_to_kafka = execute_query_for_massive_select_to_kafka,
    transfer_data_to_scd_table_on_cluster_cb = transfer_data_to_scd_table_on_cluster_cb,
    reverse_history_in_scd_table_on_cluster = reverse_history_in_scd_table_on_cluster,
    get_storage_space_schema = get_storage_space_schema,
    dependencies = {
        'cartridge.roles.crud-router',
        'cartridge.roles.vshard-router'
    }
}
