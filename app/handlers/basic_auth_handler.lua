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
local digest = require('digest')

local users = nil --Users repo

local function basic_auth_handler(req)
    local auth = req:header('authorization')
    if not auth or not auth:find('Basic ') then
      return {
        status = 401,
        body = json.encode({message = 'Missing Authorization Header'})
      }
    end

  local base64_credentials = auth:split(' ')[2]
  local credentials = digest.base64_decode(base64_credentials)
  local username = credentials:split(':')[1]
  local password = credentials:split(':')[2]


  local user = users.authenticate(username, password)
  if not user then
    return {
      status = 401,
      body = json.encode({message = 'Invalid Authentication Credentials'})
    }
  end

  req.user = user

  return req:next()
end

return {
  basic_auth_handler = basic_auth_handler
}