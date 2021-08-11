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
--- Created by ikuznetsov.
--- DateTime: 11/08/21 9:57 PM
---
local config_utils = require('app.utils.config_utils')

---query profiler config
local query_prof_opts = {
    enable_debug = false,

    is_query_debug_enable = function(self)
        return self.enable_debug
    end,

    clear = function(self)
        self.enable_debug = false
    end
}

---init query prof options
local function get_query_prof_opts()
    query_prof_opts:clear()
    local conf = config_utils.get_config()
    if conf == nil then
        return {}
    end

    if conf['query_debug'] ~= nil then
        query_prof_opts.enable_debug = conf['query_debug']
    end

    return query_prof_opts
end

return {
    get_query_prof_opts = get_query_prof_opts
}