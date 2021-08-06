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

local json = require('json')
local config_utils = require('app.utils.config_utils')
local set = require('app.entities.set')

local restrict_methods_list = set.Set {'kafka_bootstrap','kafka_consume','kafka_produce','kafka_schema_registry',
'kafka_topics', 'scheduler_tasks','schema'}

local function cluster_config_handler()

    local result = {}

    for k,v in pairs(config_utils.get_config()) do
        if restrict_methods_list[k] then
            result[k] = {['body'] = v}
        end
    end


    return {
        status = 200,
        headers = { ['content-type'] = 'application/json' },
        body = json.encode(result)}

end


return {
    cluster_config_handler = cluster_config_handler
}