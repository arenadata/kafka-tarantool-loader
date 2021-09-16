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
--- Created by Igor Kuznetsov.
--- DateTime: 2021-09-09
---

local error_repository = require("app.messages.error_repository")
local migration_selector = require("app.utils.migration_selector")
local cartridge = require("cartridge")
local pool = require("cartridge.pool")
local yaml = require("yaml")
local log = require("log")

local function migrate(req)
    local space = req:stash("tableName")

    if space == nil then
        return error_repository.return_http_response("API_MIGRATION_EMPTY_SPACE")
    end

    local body = req:json()
    local operation_type = body.operation_type

    if operation_type == nil then
        return error_repository.return_http_response("API_MIGRATION_EMPTY_TYPE")
    end

    if body.name == nil then
        return error_repository.return_http_response("API_MIGRATION_EMPTY_NAME")
    end

    local m = migration_selector.new(space)
    local func_name, func_params, err = m:get_function(body)
    if err ~= nil then
        return error_repository.return_http_response("API_MIGRATION_SELECTOR_ERROR", nil, err)
    end

    local storages = cartridge.rpc_get_candidates("app.roles.adg_storage", { leader_only = true })

    local futures = {}

    for _, cand in ipairs(storages) do
        local conn, err = pool.connect(cand)
        if conn == nil then
            return false, err
        else
            local future = conn:call(func_name, func_params, { is_async = true })
            table.insert(futures, future)
        end
    end

    local new_schema
    for _, future in ipairs(futures) do
        future:wait_result(_G.api_timeouts:get_migration_operation_timeout())
        local res, err = future:result()

        if res == nil then
            log.error(err)
            return error_repository.return_http_response("API_MIGRATION_RPC_ERROR", nil, err)
        end

        if res[1] == false or res[1] == nil then
            log.error(res[2])
            return error_repository.return_http_response("API_MIGRATION_RPC_ERROR", nil, res[2])
        end

        new_schema = res[1]
    end

    local _, err = cartridge.set_schema(yaml.encode(new_schema))
    if err ~= nil then
        return error_repository.return_http_response("API_MIGRATION_APPLY_SCHEMA_ERROR", nil, err)
    end

    return { status = 200 }
end

return {
    migrate = migrate,
}
