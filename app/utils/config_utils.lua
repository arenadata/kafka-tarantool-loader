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

local yaml = require('yaml')
local checks = require('checks')
local cartridge = require('cartridge')
local file_utils = require('app.utils.file_utils')
local misc_utils = require('app.utils.misc_utils')
local log = require('log')

local function parse_file_conf(file)
checks('string')

    local config,err = pcall(file_utils.read_file(file))

    if err == nil then
        local res, err =  yaml.decode(config)

        if res == nil then
            return nil, err
        end

        return res
    end

    return nil,err

end

local function get_config()

    local cluster_config = cartridge.config_get_deepcopy()

    if cluster_config == nil then
        return parse_file_conf('/usr/share/tarantool/memstorage/conf/kafka_config.yml')

    else return cluster_config
    end
end

local function get_topics_x_targets(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_topics'] == nil then
        return {}
    end
    local topics = conf['kafka_topics']
    if type(topics) == 'string' then
        topics = {}
    end
    local result = {}
    for k,v in pairs(topics) do
        result[k] = v['target_table']
    end
    return result
end

local function get_topics_x_schema_key(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_topics'] == nil then
        return {}
    end
    local topics = conf['kafka_topics']
    if type(topics) == 'string' then
        topics = {}
    end
    local result = {}
    for k,v in pairs(topics) do
        result[k] = v['schema_key']
    end
    return result
end

local function get_topics_x_schema_data(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_topics'] == nil then
        return {}
    end
    local topics = conf['kafka_topics']
    local result = {}
    if type(topics) == 'string' then
        topics = {}
    end
    for k,v in pairs(topics) do
        result[k] = v['schema_data']
    end
    return result
end


local function get_topics_x_error(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_topics'] == nil then
        return {}
    end
    local topics = conf['kafka_topics']
    if type(topics) == 'string' then
        topics = {}
    end
    local result = {}
    for k,v in pairs(topics) do
        result[k] = v['error_topic']
    end
    return result
end


local function get_topics_x_success(conf)

    if conf == nil then
        return {}
    end

    if conf['kafka_topics'] == nil then
        return {}
    end
    local topics = conf['kafka_topics']
    local result = {}
    if type(topics) == 'string' then
        topics = {}
    end
    for k,v in pairs(topics) do
        result[k] = v['success_topic']
    end
    return result
end

local function get_consumer_options(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_consume'] == nil then
        return {}
    end
    local consumers = conf['kafka_consume']
    local properties = consumers['properties']
    local custom_properties = consumers['custom_properties']
   -- if custom_properties ~= nil then
   --     for kc,vc in pairs(custom_properties) do
    --        properties[kc] = vc
     --   end
    --end
    if type(properties) == 'string' then properties = {} end
    return properties
end

local function get_consumer_x_type(conf)
    if conf == nil then
        return {}
    end
    if conf['kafka_consume'] == nil then
        return {}
    end
    local consumers = conf['kafka_consume']
    local result = {}
    for k,v in pairs(consumers) do
        result[k] = v['consumer_type']
    end
    return result
end


local function get_kafka_bootstrap(conf)
    if conf == nil then
        return '127.0.0.1'
    end
     if conf['kafka_bootstrap'] == nil then
        return '127.0.0.1'
    else return conf['kafka_bootstrap']['bootstrap_connection_string']
    end
end

local function get_consumer_x_topic(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_consume'] == nil then
        return {}
    end
    local consumers = conf['kafka_consume']
    local result = {}
    for k,v in pairs(consumers) do
        result[k] = v['topics']
    end
    return result;
end

local function get_all_topics(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_consume'] == nil then
        return {}
    end
    local consumers = conf['kafka_consume']
    return consumers['topics'] or {};
end


local function get_ddl_schema(conf)
    if conf == nil then
        return {}
    end
    if conf['schema'] == nil then
        return {}
    end
    local schema = conf['schema']
    return schema
end


local function contains(t, e)
    for i = 1,#t do
      if t[i] == e then return true end
    end
    return false
  end


local function check_topic_def(topics_x_targets,consumer_x_topic)
    for _,v in pairs(consumer_x_topic) do
        for _,t in ipairs(v) do
            if topics_x_targets[t] == nil then
                return false
            end
        end
    end

    return true
end

local function check_target_spaces_def(topics_x_targets, spaces)
    for _,v in pairs(topics_x_targets) do
        if not contains(spaces,v) then
        return false
        end
    end
    return true;
end

local function get_scheduler_tasks(conf)

    if conf == nil then
        return {}
    end

    if conf['scheduler_tasks'] == nil then
        return {}
    end
    local tasks = conf['scheduler_tasks']
    return tasks
end

local function get_schema_registry_opts(conf)
    if conf == nil then
        return {}
    end

    if conf['kafka_schema_registry'] == nil then
        return {}
    end
    local schema_registry = conf['kafka_schema_registry']
    return schema_registry
end



local function print_conf(conf)
    local topics_x_targets = get_topics_x_targets(conf)
    local consumer_options = get_consumer_options(conf)
    local consumer_x_type = get_consumer_x_type(conf)
    local consumer_x_topic = get_consumer_x_topic(conf)
    local spaces = get_ddl_schema(conf)

    print(check_topic_def(topics_x_targets,consumer_x_topic))
    print(check_target_spaces_def(topics_x_targets,spaces))


    misc_utils.print_table(topics_x_targets)
    misc_utils.print_table(consumer_options)
    misc_utils.print_table(consumer_x_type)
    misc_utils.print_table(consumer_x_topic)
    misc_utils.print_table(spaces)


end


return {
    get_kafka_bootstrap = get_kafka_bootstrap,
    get_config = get_config,
    get_all_topics = get_all_topics,
    get_consumer_options = get_consumer_options,
    get_ddl_schema = get_ddl_schema,
    get_topics_x_targets = get_topics_x_targets,
    get_scheduler_tasks = get_scheduler_tasks,
    print_conf = print_conf,
    parse_file_conf = parse_file_conf,
    get_schema_registry_opts= get_schema_registry_opts,
    get_topics_x_schema_key =get_topics_x_schema_key,
    get_topics_x_schema_data = get_topics_x_schema_data,
    get_topics_x_error = get_topics_x_error,
    get_topics_x_success = get_topics_x_success
}