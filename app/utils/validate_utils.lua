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

local error_repository = require('app.messages.error_repository')
local checks = require('checks')

--TODO !!! Generic Check Type Function and ERROR !!!

---check_kafka_bootstrap_definition - method, that checks kafka_bootstrap_settings.
---@param kafka_bootstrap_settings - setting to check.
---@return boolean|string - true|nil or false|error
local function check_kafka_bootstrap_definition(kafka_bootstrap_settings)

    if kafka_bootstrap_settings == nil then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_001')
    end

    if type(kafka_bootstrap_settings) ~= 'table' then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_002')
    end

    local bootstrap_string = kafka_bootstrap_settings['bootstrap_connection_string']

    if bootstrap_string == nil then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_003')
    end

    if type(bootstrap_string) ~= 'string' then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_004')
    end

    return true, nil
end


---check_topic_definition
---@param topics_list table
---@param topic_options table
---@return boolean|string
local function check_topic_definition(topics_list, topic_options)

    if topics_list == nil then
        return false,error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_005')
    end

    if topic_options == nil then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_006')
    end

    if type(topics_list) ~= 'table' then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_007')
    end

    if type(topic_options) ~= 'table' then
        return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_008')
    end

    local required_options = {'target_table','schema_key', 'schema_data', 'error_topic', 'success_topic'}
    for _,topic in ipairs(topics_list) do
        local cur_consumer_option = topic_options[topic]
        if cur_consumer_option == nil then
            return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_009',{topic=topic})
        end
        for _,r in ipairs(required_options) do
            if cur_consumer_option[r] == nil then
                return false, error_repository.get_error_code('ADG_KAFKA_CONNECTOR_VALIDATION_010',{topic=topic,
                                                                                                    required_option =r})
            end
        end
    end
    return true, nil
end

---check_options_for_type
---@param options table
---@param check_type string
---@return boolean|string
local function check_options_for_type(options,check_type)
    checks('table','string')
    for _,argument in ipairs(options) do
        if type(argument) == 'string' then
            goto continue
        end
        for key,elem in pairs(argument) do
            if type(elem) ~= check_type or type(key) ~= check_type then
                return false, error_repository.get_error_code(
                        'ADG_KAFKA_CONNECTOR_VALIDATION_011',{elem=type(elem),key=type(key)})
            end
        end
        ::continue::
    end
    return true, nil
end

---check_schema_registry_opts
---@param schema_registry_opts table
---@return boolean|string
local function check_schema_registry_opts(schema_registry_opts)
    if schema_registry_opts == nil then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_001')
    end

    if type(schema_registry_opts) ~= 'table' then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_002')
    end

    local schema_registry_host = schema_registry_opts['host']
    local schema_registry_port = schema_registry_opts['port']

    if schema_registry_host == nil then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_003')
    end

    if type(schema_registry_host) ~= 'string' then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_004')
    end

    if schema_registry_port == nil then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_005')
    end

    if type(schema_registry_port) ~= 'number' then
        return false, error_repository.get_error_code('SCHEMA_REGISTRY_VALIDATION_006')
    end

    --TODO Service health check?
    return true, nil
end

---check_etl_opts
---@param etl_opts table
---@return boolean|string
local function check_etl_opts(etl_opts)
    if etl_opts == nil then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_001')
    end

    if type(etl_opts) ~= 'table' then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_002')
    end

    local start_field_nm = etl_opts['start_field_nm']
    local end_field_nm = etl_opts['end_field_nm']
    local op_field_nm = etl_opts['op_field_nm']

    if start_field_nm == nil then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_003')
    end

    if type(start_field_nm) ~= 'string' then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_004')
    end

    if end_field_nm == nil then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_005')
    end

    if type(end_field_nm) ~= 'string' then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_006')
    end

    if op_field_nm == nil then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_007')
    end

    if type(op_field_nm) ~= 'string' then
        return false,error_repository.get_error_code('ETL_SCD_VALIDATION_008')
    end
    return true, nil
end

return {
    check_kafka_bootstrap_definition = check_kafka_bootstrap_definition,
    check_topic_definition = check_topic_definition,
    check_options_for_type = check_options_for_type,
    check_schema_registry_opts = check_schema_registry_opts,
    check_etl_opts = check_etl_opts
}