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
--- Created by ashitov.
--- DateTime: 4/20/20 1:00 PM
---
local t = require('luatest')
local g = t.group('entities.set')
local set = require('app.entities.set')

g.test_simple_cases = function ()
    local list1 = {'a','b','c','d'}
    local list2 = {'a','a','a'}
    local list3 = {}

    local set1 = set.Set(list1)
    local set2 = set.Set(list2)
    local set3 = set.Set(list3)

    t.assert_equals(set1,{a = true, b = true, c = true, d = true})
    t.assert_equals(set2,{a = true})
    t.assert_equals(set3,{})


end

g.test_bad_input = function()
    t.assert_error(set.Set,nil)
end