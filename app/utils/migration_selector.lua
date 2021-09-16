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
--- DateTime: 2021-09-15

local checks = require("checks")
local error_repository = require("app.messages.error_repository")

local migration_selector = {
    function_map = {
        create_index = { func_name = "index_create", param_func = "_get_create_index_params" },
        drop_index = { func_name = "index_drop", param_func = "_get_drop_index_params" },
        add_column = { func_name = "add_columns", param_func = "_get_add_column_params" },
        drop_column = { func_name = "drop_columns", param_func = "_get_drop_column_params" },
    },
    space = nil,

    set_space = function(self, space)
        self.space = space
    end,

    get_function = function(self, body)
        checks("table", "table")

        if body.operation_type == nil then
            return nil, nil, error_repository.get_error_code("API_MIGRATION_EMPTY_TYPE")
        end

        if body.name == nil then
            return nil, nil, error_repository.get_error_code("API_MIGRATION_EMPTY_NAME")
        end

        local f = self.function_map[body.operation_type]
        if f == nil then
            return nil, nil, error_repository.get_error_code("API_MIGRATION_UNKNOWN_TYPE")
        end

        return f.func_name, self[f.param_func](self, body), nil
    end,

    _get_create_index_params = function(self, body)
        return {
            self.space,
            body.name,
            body.params,
        }
    end,

    _get_drop_index_params = function(self, body)
        return {
            self.space,
            body.name,
        }
    end,

    _get_add_column_params = function(self, body)
        return {
            self.space,
            { { name = body.name, type = body.params.type, is_nullable = body.params.is_nullable } },
        }
    end,

    _get_drop_column_params = function(self, body)
        return {
            self.space,
            { body.name },
        }
    end,
}

local function new(space)
    migration_selector:set_space(space)
    return migration_selector
end

return {
    new = new,
}
