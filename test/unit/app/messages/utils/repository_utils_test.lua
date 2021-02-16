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
--- DateTime: 4/17/20 2:04 PM
---

local repository_utils = require('app.messages.utils.repository_utils')
local t = require('luatest')

local g = t.group('repository_utils.reload_repo_from_files')

g.test_read_simple_configs = function ()
    local files = {'test/unit/data/config_files/ADG_INPUT_PROCESSOR.yml',
                   'test/unit/data/config_files/ADG_KAFKA_CONNECTOR.yml'}

    local options = repository_utils.reload_repo_from_files(files)


    t.assert_covers(options,{['ADG_INPUT_PROCESSOR_001'] = {
        status = 'error' ,
        errorCode = 'ADG_INPUT_PROCESSOR_001',
        error = 'ERROR: Cannot find parse function'
    }})

    t.assert_covers(options,{['ADG_KAFKA_CONNECTOR_002'] = {
        status = 'error' ,
        errorCode = 'ADG_KAFKA_CONNECTOR_002',
        error = 'ERROR: Did not receive rows from input processor'
    }})
end

g.test_read_empty_configs = function()
    local options = repository_utils.reload_repo_from_files({})
    t.assert_equals(options,{})
end

g.test_read_not_yaml_configs = function()
    local files = {'test/unit/data/simple_files/multiline.txt','test/unit/data/simple_files/singleline.txt'}
    local options = repository_utils.reload_repo_from_files(files)
    t.assert_equals(options,{})
end