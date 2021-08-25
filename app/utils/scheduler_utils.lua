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

local config_utils = require("app.utils.config_utils")
local tasks_table = {}
local cron = require("cron")
local log = require("log")
local checks = require("checks")

local function init_scheduler_tasks()
    tasks_table = config_utils.get_scheduler_tasks(config_utils.get_config())
end

local function get_periodical_tasks(tasks)
    checks("table")
    local parsed = {}
    local result = {}
    for _, v in pairs(tasks) do
        if v["kind"] == "periodical" then
            do
                local schedule = v["schedule"]
                local role_name = v["rolename"]
                local funcname = v["funcname"]
                local args = v["args"]

                if parsed[schedule] == nil then
                    parsed[schedule] = { { role_name, funcname, args } }
                else
                    table.insert(parsed[schedule], { role_name, funcname, args })
                end
            end
        end
    end

    for k in pairs(parsed) do
        local parsed_schedule, err = cron.parse(k)
        if not parsed_schedule then
            log.error("ERROR: Cron parsing error " .. err)
        else
            result[cron.next(parsed_schedule)] = parsed[k]
        end
    end

    return result
end

local function get_current_periodical_tasks()
    return get_periodical_tasks(tasks_table)
end

return {
    init_scheduler_tasks = init_scheduler_tasks,
    get_periodical_tasks = get_periodical_tasks,
    get_current_periodical_tasks = get_current_periodical_tasks,
}
