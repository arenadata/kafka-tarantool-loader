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

local config_utils = require('app.utils.config_utils')
local brokers =  {}
local topics = {}
local options = {}


local function init_kafka_opts()
     brokers = config_utils.get_kafka_bootstrap(config_utils.get_config())
     topics = config_utils.get_all_topics(config_utils.get_config())
     options = config_utils.get_consumer_options(config_utils.get_config())

end

local function get_brokers()
    return brokers
end


local function get_topics()
    return topics
end

local function get_options()
    return options
end


return {
    init_kafka_opts = init_kafka_opts,
    get_brokers = get_brokers,
    get_topics = get_topics,
    get_options = get_options
}

