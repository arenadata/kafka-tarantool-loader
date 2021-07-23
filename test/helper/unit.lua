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

local t = require('luatest')

local shared = require('test.helper')

local helper = {shared = shared}
local success_repository = require('app.messages.success_repository')
local error_repository = require('app.messages.error_repository')

t.before_suite(function()
    box.cfg({work_dir = shared.datadir})
    box.schema.user.grant(
            'guest', 'read,write,execute', 'universe', nil, { if_not_exists = true }
    )
    error_repository.init_error_repo('en')
    success_repository.init_success_repo('en')
end)


return helper