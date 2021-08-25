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
local digest = require("digest")
local ffi = require("ffi")

local function dtm_int32_hash(input)
    checks("string")
    return ffi.cast("int32_t*", ffi.new("const char*", digest.md5_hex(input):sub(1, 4)))[0]
end

local function convert_lua_types_for_checksum(value, delimeter)
    local value_type = type(value)
    local string_value = tostring(value)
    if value_type == "nil" or value == nil then
        return ""
    elseif value_type == "number" then
        return string_value
    elseif value_type == "string" then
        return value
    elseif value_type == "boolean" then
        if value == false then
            return "0"
        else
            return "1"
        end
    elseif value_type == "userdata" then
        return string_value
    elseif value_type == "cdata" then
        if string.find(string_value, "ULL") then
            local result = string_value:gsub("ULL", "")
            return result
        elseif string.find(string_value, "LL") then
            local result = string_value:gsub("LL", "")
            return result
        else
            return string_value
        end
    elseif value_type == "table" then
        local res = {}
        for _, v in pairs(value) do
            table.insert(res, convert_lua_types_for_checksum(v, delimeter))
        end
        return table.concat(res, delimeter)
    elseif value_type == "function" then
        return ""
    elseif value_type == "thread" then
        return ""
    else
        return string_value
    end
end

return {
    dtm_int32_hash = dtm_int32_hash,
    convert_lua_types_for_checksum = convert_lua_types_for_checksum,
}
