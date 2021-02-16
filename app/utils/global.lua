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


local checks = require('checks')

if rawget(_G, "_global_default") == nil then
    _G._global_default = {}
end

if rawget(_G, "_global_values") == nil then
    _G._global_values = {}
end

local function new(self, name, value)
    checks("table", "string", "?")

    local default = _G._global_default

    if default[self.module_name] == nil then
        default[self.module_name] = {}
    end

    default[self.module_name][name] = value or {}
end

local function newindex(self, name, value)
    checks("table", "string", "?")

    local global_values = _G._global_values

    local module_values = global_values[self.module_name]
    if module_values == nil then
        global_values[self.module_name] = {}
        module_values = global_values[self.module_name]
    end

    module_values[name] = value
end

local function index(self, name)
    checks("table", "string")

    local global_values = _G._global_values

    if global_values[self.module_name] ~= nil then
        local res = global_values[self.module_name][name]
        if res ~= nil then
            return res
        end
    else
        global_values[self.module_name] = {}
    end

    local default = _G._global_default

    if default[self.module_name] == nil then
        default[self.module_name] = {}
    end

    local default_value = default[self.module_name][name]

    global_values[self.module_name][name] = default_value

    return default_value
end

local function create(module_name)
    checks("string")

    local obj = {
        module_name = module_name,
        new = new
    }
    setmetatable(
        obj, {
            __newindex = newindex,
            __index = index
        })
    return obj
end

return { new = create }
