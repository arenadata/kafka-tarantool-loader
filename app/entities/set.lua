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

---Set - method to create set in Lua.
---@param list table - list of entities, that need to convert to set.
---@return table - table, that have structure set[item] = true
local function Set (list)
    checks('table')
    local set = {}
    for _, l in ipairs(list) do set[l] = true end
    return set
  end


  return {
      Set = Set
  }