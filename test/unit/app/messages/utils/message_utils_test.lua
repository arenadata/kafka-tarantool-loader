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
--- DateTime: 4/17/20 2:03 PM
---

local message_utils = require('app.messages.utils.message_utils')
local json = require('json')
local t = require('luatest')

local g = t.group('message_utils.get_message_code')


g.test_get_from_valid_repo = function()
    local  repo = {['ADG_INPUT_PROCESSOR_001'] = {
        status = 'error' ,
        errorCode = 'ADG_INPUT_PROCESSOR_001',
        error = 'ERROR: Cannot find parse function'
    }}

    local value = message_utils.get_message_code(repo,'ADG_INPUT_PROCESSOR_001')
    t.assert_equals(value,json.encode(repo['ADG_INPUT_PROCESSOR_001']))

    local value2 = message_utils.get_message_code(repo,'ADG_INPUT_PROCESSOR_002')
    t.assert_equals(value2,json.encode({'ERROR: code ADG_INPUT_PROCESSOR_002 not found'}))

    local value3 = message_utils.get_message_code(repo,'ADG_INPUT_PROCESSOR_001',{test = 'test'})
    local test_value = repo['ADG_INPUT_PROCESSOR_001']
    test_value['opts'] = {test = 'test'}
    t.assert_equals(value3,json.encode(test_value))

end

g.test_invalid_import = function ()
    t.assert_error(message_utils.get_message_code,nil,nil)
    t.assert_error(message_utils.get_message_code,{},nil)

    local value = message_utils.get_message_code({}, '')
    t.assert_equals(value,json.encode({'ERROR: code  not found'}))
end