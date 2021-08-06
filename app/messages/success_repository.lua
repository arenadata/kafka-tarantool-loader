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
local log = require('log')
local message_utils = require('app.messages.utils.message_utils')
local file_utils = require('app.utils.file_utils')
local repository_utils = require('app.messages.utils.repository_utils')
local fio = require('fio')

---@type table - that contains success operations messages.
local success_codes = {}

---@type table - that contains success http-response codes.
local http_response_codes = {}

---init_success_repo - initialization method, that load success messages repository from files in 'app/messages/repos/'.
---@param language string - language of files to seek.
local function init_success_repo(language)
    checks('string')
    local path_to_repo = fio.abspath(fio.dirname(arg[0])) .. '/app/messages/repos/'
    local success_repo_files_path = path_to_repo .. language .. '/success'
    local success_repo_files = file_utils.get_files_in_directory(success_repo_files_path, '*.yml')
    success_codes = repository_utils.reload_repo_from_files(success_repo_files)
    local success_repo_resp_files_path = success_repo_files_path .. '/response_codes'
    local success_repo_resp_files = file_utils.get_files_in_directory(success_repo_resp_files_path, '*.yml')
    http_response_codes = repository_utils.reload_repo_from_files(success_repo_resp_files)
end

---get_success_code - the method, that returns the success message in JSON-string format
---with additional opts from error messages repository.
---@param code string - repository message code.
---@param opts table - additional opts.
---@return string - the error in json-string format.
local function get_success_code(code,opts)
    checks('string', '?table')
    local succ = message_utils.get_message_code(success_codes,code,opts)
    log.info(succ)
    return succ
end

---return_http_response - the method, that returns http-response with success message.
---@param code string - repository message code.
---@param body string - http-response message body.
---@return table - http-response with content-type = application/json.
local function return_http_response(code,body)
    checks('string','?string')
    local http_code = message_utils.get_message_code(http_response_codes,code)
    if body == nil then
        body = get_success_code(code)
    end
    return {
        status = tonumber(http_code),
        headers = { ['content-type'] = 'application/json' },
        body = body}
end


return {
    get_success_code = get_success_code,
    return_http_response = return_http_response,
    init_success_repo = init_success_repo
}