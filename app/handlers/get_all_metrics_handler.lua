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

local cartridge = require("cartridge")
local pool = require("cartridge.pool")
local json = require("json")
local log = require("log")

local function get_all_metrics()
    local result = {}
    local topology = cartridge.admin_get_servers()

    for _, server in ipairs(topology) do
        local current_uri = server["uri"]

        if server["replicaset"] ~= nil then
            local conn, err = pool.connect(current_uri)
            if conn == nil then
                log.error(err)
            end

            local res = conn:call("get_metric", {}, { is_async = false })
            result[server["alias"]] = { roles = server["replicaset"]["roles"], metrics = res }
        end
    end

    return {
        status = 200,
        headers = { ["content-type"] = "application/json" },
        body = json.encode(result),
    }
end

return {
    get_all_metrics = get_all_metrics,
}
