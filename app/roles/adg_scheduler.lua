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
local scheduler_utils = require('app.utils.scheduler_utils')
local clock = require('clock')
local log = require('log')
local fiber = require('fiber')
local pool = require('cartridge.pool')
local schema_utils = require('app.utils.schema_utils')
local vshard = require('vshard')
local error_repository = require('app.messages.error_repository')
local success_repository = require('app.messages.success_repository')
local metrics = require('app.metrics.metrics_storage')

local cartridge_pool = require('cartridge.pool')
local cartridge_rpc = require('cartridge.rpc')

local json = require('json')

local role_name = 'app.roles.adg_scheduler'

local garbage_fiber = nil

_G.event_loop_run = nil
_G.get_metric = nil

local schedule_fiber = nil

local function stop()
    schedule_fiber:cancel()
    garbage_fiber:cancel()
    return true
end


local function validate_config(conf_new, conf_old)
    if type(box.cfg) ~= 'function' and not box.cfg.read_only then
        if conf_new.schema ~= nil then
        end
    end
    return true
end


local function apply_config(conf, opts) -- luacheck: no unused args
    if opts.is_master and  pcall(vshard.storage.info) == false then
        schema_utils.drop_all() 
        if conf.schema ~= nil then
        end
    end
    if schedule_fiber ~= nil and schedule_fiber:status() ~= 'dead' then
        schedule_fiber:cancel()
    end
    scheduler_utils.init_scheduler_tasks()
    schedule_fiber = fiber.create(event_loop_run)
    if schedule_fiber ~= nil and schedule_fiber:status() ~= 'dead' then
        schedule_fiber:name('SCHEDULER_FIBER')
    end
    error_repository.init_error_repo('en')
    success_repository.init_success_repo('en')
    return true
end

local function event_loop_run()
    local tasks = scheduler_utils.get_current_periodical_tasks() or {}
    while true do
        --log.info('INFO: Event loop tick')
        for k,v in pairs(tasks) do
            local current_ts = math.floor(clock.time())
            if(k == current_ts) then do
                for _,v2 in ipairs(v) do
                log.info('INFO: Starting task ' .. v2[2] .. ' on ' .. v2[1])
                local candidates =  cartridge.rpc_get_candidates(v2[1])
                if #candidates == 0 then
                    log.error('ERROR: candidate not found')
                end
                for _,cand in ipairs(candidates) do
                    log.info('INFO: Candidates for task ' .. cand)
                   local conn, err = pool.connect(cand)
                   if conn == nil then
                        log.error(err)
                   else
                    conn:call(v2[2],v2[3],{is_async=true})
                    end
                end
                end
            end
            end
            tasks = scheduler_utils.get_current_periodical_tasks() or {}

    end
    fiber.sleep(1)
    end
end

local function get_metric()
    return metrics.export(role_name)
end

local function get_schema()
    for _, instance_uri in pairs(cartridge_rpc.get_candidates('app.roles.adg_storage', { leader_only = true })) do
        return cartridge_rpc.call('app.roles.adg_storage', 'get_schema', nil, { uri = instance_uri })
    end
end

local function init(opts)
    rawset(_G, 'ddl', { get_schema = get_schema })

    _G.event_loop_run = event_loop_run
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
    get_metric = get_metric,
    get_schema = get_schema,
    dependencies = {
        'cartridge.roles.crud-router',
        'cartridge.roles.vshard-router'
    }
}
