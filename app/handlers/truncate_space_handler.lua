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

local cartridge = require('cartridge')
local pool = require('cartridge.pool')
local json = require('json')
local log = require('log')

local url_utils = require('app.utils.url_utils')
local error_repository = require('app.messages.error_repository')
local success_repository = require('app.messages.success_repository')

local function truncate_space_on_cluster(req)
    local space_name = url_utils.url_decode(req:query_param('_space_name'))
    if space_name == nil then
        return error_repository.return_http_response('TRUNCATE_SPACE_ON_CLUSTER_001')
    end

    local ok,err = _G.truncate_space_on_cluster(space_name)

    if ok then return success_repository.return_http_response('TRUNCATE_SPACE_ON_CLUSTER_001')
    else error_repository.return_http_response('TRUNCATE_SPACE_ON_CLUSTER_002', {error = err})
    end
end


return {
    truncate_space_on_cluster = truncate_space_on_cluster
}