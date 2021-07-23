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

local avro_schema = require('avro_schema')
local checks = require('checks')
local misc_utils = require('app.utils.misc_utils')


local function check_avro_schema_compatibility(old_schema, new_schema, opts)
    checks('table', 'table', {
        is_downgrade = '?boolean'
    })
    local downgrade_string = nil

    if opts['is_downgrade'] == true then
        downgrade_string = 'downgrade'
    end

    local ok = avro_schema.are_compatible(old_schema,new_schema,downgrade_string)
    return ok

end


local function validate_avro_data(schema, data)

    if not avro_schema.is(schema) then
        return false,nil
    end

    local ok,res = avro_schema.validate(schema,data)

    return ok,res
end

local function compile_avro_schema(schema)
    checks('table')
    if not avro_schema.is(schema) then
        return false, nil
    end

    local ok, methods = avro_schema.compile(schema)

    return ok,methods

end



return {
    check_avro_schema_compatibility = check_avro_schema_compatibility,
    validate_avro_data = validate_avro_data,
    compile_avro_schema = compile_avro_schema
}