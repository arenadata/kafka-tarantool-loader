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
--- DateTime: 30/7/21 7:57 PM
---
local config_utils = require('app.utils.config_utils')

local api_timeout_opts = {
    timeouts = {
        transfer_stage_data_to_scd_tbl=86400, -- 1 day
        scd_table_checksum=86400
    },

    get_transfer_stage_data_to_scd_table_timeout = function(self)
        return self.timeouts.transfer_stage_data_to_scd_tbl
    end,

    get_scd_table_checksum_timeout = function(self)
        return self.timeouts.scd_table_checksum
    end,

    clear = function(self)
        self.timeouts = {
            transfer_stage_data_to_scd_tbl=86400,
            scd_table_checksum=86400
        }
    end
}

local function get_api_timeout_opts()
    local conf = config_utils.get_config()
    if conf == nil then
        return {}
    end

    if conf['api_timeout'] ~= nil then
        api_timeout_opts.timeouts = conf['api_timeout']
    end

    return api_timeout_opts
end

return {
    get_api_timeout_opts = get_api_timeout_opts
}