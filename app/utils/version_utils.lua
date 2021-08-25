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

local json = require("json")
local file_utils = require("app.utils.file_utils")
local lpeg = require("lulpeg")

local version = nil

local function get_tarantool_version()
    return require("tarantool").version
end

local function get_cartridge_version()
    return require("cartridge").VERSION
end

local function get_memstorage_version()
    local rockspecs = file_utils.get_files_in_directory(package.searchroot(), "*.rockspec")
    if #rockspecs == 0 then
        return "unknown"
    end
    local rockspec, _ = file_utils.read_file(rockspecs[1])
    if rockspec == nil then
        return "unknown"
    end

    local space = lpeg.S(" \t\r\n") ^ 0
    local quotation = (lpeg.P("'") + lpeg.P('"')) ^ 1
    local not_version = (lpeg.P(1) - lpeg.P("version")) ^ 0
    local pattern = not_version
        * lpeg.P("version")
        * space
        * lpeg.P("=")
        * space
        * quotation
        * lpeg.C((lpeg.R("09") + lpeg.S("-.,")) ^ 0)
        * quotation
        * not_version
    return pattern:match(rockspec) or "unknown"
end

local function get_version_json()
    if version == nil then
        version = json.encode({
            {
                ["name"] = "tarantool",
                ["version"] = get_tarantool_version(),
            },
            {
                ["name"] = "tarantool-cartridge",
                ["version"] = get_cartridge_version(),
            },
            {
                ["name"] = "kafka-tarantool connector",
                ["version"] = get_memstorage_version(),
            },
        })
        return version
    else
        return version
    end
end

return {
    get_tarantool_version = get_tarantool_version,
    get_cartridge_version = get_cartridge_version,
    get_memstorage_version = get_memstorage_version,
    get_version_json = get_version_json,
}
