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

local cartridge_helpers = require('cartridge.test-helpers')
local shared = require('test.helper')

local helper = {shared = shared}
local file_utils = require('app.utils.file_utils')
local yaml = require('yaml')

helper.cluster = cartridge_helpers.Cluster:new({
    server_command = shared.server_command,
    datadir = shared.datadir,
    use_vshard = true,
    replicasets = {
        {
            alias = 'api',
            uuid = cartridge_helpers.uuid('b'),
            roles = {'app.roles.adg_api'},
            servers = {{ instance_uuid = cartridge_helpers.uuid('b', 1) }},
        },
        {
            alias = 'master-1',
            uuid = cartridge_helpers.uuid('a'),
            roles = {'app.roles.adg_storage'},
            servers = {{ instance_uuid = cartridge_helpers.uuid('a', 1) }},
        },
        {
            alias = 'master-2',
            uuid = cartridge_helpers.uuid('f'),
            roles = {'app.roles.adg_storage'},
            servers = {{ instance_uuid = cartridge_helpers.uuid('f', 1) }},
        },
        {
            alias = 'input_processor',
            uuid = cartridge_helpers.uuid('c'),
            roles = {'app.roles.adg_input_processor'},
            servers = {{instance_uuid = cartridge_helpers.uuid('c',1)}}
        },
        {
            alias = 'output_processor',
            uuid = cartridge_helpers.uuid('d'),
            roles = {'app.roles.adg_output_processor'},
            servers = {{instance_uuid = cartridge_helpers.uuid('d',1)}}
        },
        {
            alias = 'kafka_connector',
            uuid = cartridge_helpers.uuid('e'),
            roles = {'app.roles.adg_kafka_connector'},
            servers = {{instance_uuid = cartridge_helpers.uuid('e',1)}}
        },
        {
            alias = 'state',
            uuid = cartridge_helpers.uuid('ab'),
            roles = {'app.roles.adg_state'},
            servers = {{instance_uuid = cartridge_helpers.uuid('ab',1)}}
        }


    },
})

local config = { ['kafka_bootstrap'] = {
    ['bootstrap_connection_string'] = '10.92.6.44:9092'
},
                 ['kafka_consume'] = {
                     ['topics'] = {},
                     ['properties'] = {["group.id"] = "tnt_consumer"},
                     ['custom_properties'] = {}

                 },
                 ['kafka_produce'] = {
                     ['properties'] = {},
                     ['custom_properties'] = {}
                 },
                 ['kafka_schema_registry'] = {
                     ['host'] = 'localhost',
                     ['port'] = 8081,
                     ['key_schema_name'] = 'AdbUploadRequest'
                 },
                 ['kafka_topics'] = {},
                 ['scheduler_tasks'] = {},
                 ['etl_scd'] = {
                     ['start_field_nm'] = 'sysFrom',
                     ['end_field_nm'] = 'sysTo',
                     ['op_field_nm'] = 'sysOp'
                 },
                 ['schema'] = {}
}

config.schema =  yaml.decode(file_utils.read_file('test/integration/data/schema_ddl.yml'))

helper.cluster_config = config

t.before_suite(function()
    helper.cluster:start()
    helper.cluster:upload_config(config)
end)
t.after_suite(function() helper.cluster:stop() end)

return helper