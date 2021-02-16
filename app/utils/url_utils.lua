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

-- ref: https://gist.github.com/ignisdesign/4323051
-- ref: http://stackoverflow.com/questions/20282054/how-to-urldecode-a-request-uri-string-in-lua
-- to encode table as parameters, see https://github.com/stuartpb/tvtropes-lua/blob/master/urlencode.lua

local char_to_hex = function(c)
    return string.format("%%%02X", string.byte(c))
  end
  
  local function url_encode(url)
    if url == nil then
      return
    end
    url = url:gsub("\n", "\r\n")
    url = url:gsub("([^%w ])", char_to_hex)
    url = url:gsub(" ", "+")
    return url
  end
  
  local hex_to_char = function(x)
    return string.char(tonumber(x, 16))
  end
  
  local function url_decode(url)
    if url == nil then
      return
    end
    url = url:gsub("+", " ")
    url = url:gsub("%%(%x%x)", hex_to_char)
    return url
  end

  return {
      url_encode = url_encode,
      url_decode = url_decode
  }