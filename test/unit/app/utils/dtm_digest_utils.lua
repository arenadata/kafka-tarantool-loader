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

local digest_utils = require('app.utils.dtm_digest_utils')
local t = require('luatest')



local g = t.group('dtm_digest_utils.dtm_int32_hash')
local g2 = t.group('dtm_digest_utils.convert_lua_types_for_checksum')


g.test_simple_cases = function ()

    t.assert_equals(digest_utils.dtm_int32_hash('12345'),1664561720)
    t.assert_equals(digest_utils.dtm_int32_hash('000000000000000000000000'),925906486)
    t.assert_equals(digest_utils.dtm_int32_hash('гнев богиня воспой Ахиллеса, Пелеева сына. многих ...'),808673840)
    t.assert_equals(digest_utils.dtm_int32_hash('sfrsfv -s&*& (#as fkjs df&^*sw jdefksfd^&^8eswr'),876164962)
    t.assert_equals(digest_utils.dtm_int32_hash('6'),959919665)
end

g2.test_simple_conversions = function ()
    local string = 'sss'
    local number = 123
    local table = {1,2,3}
    local nested_table = {1,2,3,{4,5}}
    local nested_table2 = {{-1,-2,-3},1,2,3,{4,5,{6,7}}}
    local boolean_true = true
    local boolean_false = false
    local nil_value = nil
    local long = tonumber64('-100000000000000LL')
    local ulong = tonumber64('100000000000000ULL')

    t.assert_equals(digest_utils.convert_lua_types_for_checksum(string,";"),'sss')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(number,";"),'123')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(table,";"),'1;2;3')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(nested_table,";"),'1;2;3;4;5')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(nested_table2,";"),'-1;-2;-3;1;2;3;4;5;6;7')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(boolean_true,";"),'1')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(boolean_false,";"),'0')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(long,";"),'-100000000000000')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(ulong,";"),'100000000000000')
    t.assert_equals(digest_utils.convert_lua_types_for_checksum(nil_value,";"),'')
end