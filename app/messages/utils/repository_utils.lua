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

local checks = require("checks")
local file_utils = require("app.utils.file_utils")
local yaml = require("yaml")
local fun = require("fun")
local log = require("log")

---reload_repo_from_files - the method, that parses YAML files and return single Lua table with message repository data.
---@param files table - array, that contains file paths to YAML config files.
---@return table - lua-table with message repository data.
local function reload_repo_from_files(files)
    checks("table")
    return fun.map(function(x)
        local is_file_ok, file_string = pcall(file_utils.read_file, x) --read files
        if is_file_ok then
            return file_string
        else
            return ""
        end
    end, files)
        :map(function(x)
            local is_yaml_ok, yaml_value = pcall(yaml.decode, x) -- parse yaml
            if is_yaml_ok then
                return yaml_value
            else
                log.error(yaml_value)
                return {}
            end
        end)
        :reduce(function(acc, x)
            if type(x) ~= "table" then
                return acc
            end
            for k, v in pairs(x) do
                acc[k] = v -- shrink in one file
            end
            return acc
        end, {})
end

return {
    reload_repo_from_files = reload_repo_from_files,
}
