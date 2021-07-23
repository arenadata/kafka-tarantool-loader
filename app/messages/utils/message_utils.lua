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

local checks = require('checks')
local json = require('json')
local log = require('log')


---get_message_code - method, that return message with code from messages.
---@param messages table - map, that contains message repository.
---@param code string - repository message code.
---@param opts table - additional opts.
---@return string - the message in JSON-string format.
local function get_message_code(messages,code,opts)
    checks('table','string','?table')
    local raw_message
    local message_desc = messages[code] or messages[string.upper(code)]

    if message_desc == nil then
        raw_message = {string.format('ERROR: code %s not found',code)}
    else
        if opts ~= nil then
            messages[code]['opts'] = opts
            raw_message = messages[code]
        else raw_message = messages[code]
        end
    end
    local ok, json_message =  pcall(json.encode, raw_message)

    if not ok then
        log.error('ERROR: JSON convertation failed')
    else
        return json_message
    end
end

return {
    get_message_code = get_message_code
}