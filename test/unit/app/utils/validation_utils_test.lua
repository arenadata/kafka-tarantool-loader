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
--- Created by ashitov.
--- DateTime: 5/7/20 3:02 PM
---
local validate_utils = require("app.utils.validate_utils")
local t = require("luatest")
local g = t.group("validate_utils.check_kafka_bootstrap_definition")
local g2 = t.group("validate_utils.checks_kafka_options")
local g3 = t.group("validate_utils.check_schema_registry_options")

g.test_check_invalid_input = function()
    local res, err = validate_utils.check_kafka_bootstrap_definition(nil)

    t.assert_equals(res, false)
    t.assert_str_contains(err, "ADG_KAFKA_CONNECTOR_VALIDATION_001")

    local res2, err2 = validate_utils.check_kafka_bootstrap_definition({})

    t.assert_equals(res2, false)
    t.assert_str_contains(err2, "ADG_KAFKA_CONNECTOR_VALIDATION_003")

    local res3, err3 = validate_utils.check_kafka_bootstrap_definition("bootstrap_connection_string")

    t.assert_equals(res3, false)
    t.assert_str_contains(err3, "ADG_KAFKA_CONNECTOR_VALIDATION_002")

    local res4, err4 = validate_utils.check_kafka_bootstrap_definition(123)

    t.assert_equals(res4, false)
    t.assert_str_contains(err4, "ADG_KAFKA_CONNECTOR_VALIDATION_002")

    local res5, err5 = validate_utils.check_kafka_bootstrap_definition({ ["bootstrap_connection_string"] = nil })

    t.assert_equals(res5, false)
    t.assert_str_contains(err5, "ADG_KAFKA_CONNECTOR_VALIDATION_003")

    local res6, err6 = validate_utils.check_kafka_bootstrap_definition({ ["bootstrap_connection_string"] = 123 })

    t.assert_equals(res6, false)
    t.assert_str_contains(err6, "ADG_KAFKA_CONNECTOR_VALIDATION_004")

    local res7, err7 = validate_utils.check_kafka_bootstrap_definition({ ["bootstrap_connection_string_fff"] = 123 })

    t.assert_equals(res7, false)
    t.assert_str_contains(err7, "ADG_KAFKA_CONNECTOR_VALIDATION_003")
end

g.test_check_valid_input = function()
    local res, err =
        validate_utils.check_kafka_bootstrap_definition({
            ["bootstrap_connection_string"] = "localhost:9092",
        })
    t.assert_equals(res, true)
    t.assert_equals(err, nil)
end

g2.test_check_valid_input = function()
    local input1 = {
        ["enable.auto.offset.store"] = "true",
        ["group.id"] = "example_consumer",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
    }

    local res, err = validate_utils.check_options_for_type({ input1 }, "string")
    t.assert_equals(res, true)
    t.assert_equals(err, nil)

    local input2 = {}
    local res2, err2 = validate_utils.check_options_for_type({ input2 }, "string")
    t.assert_equals(res2, true)
    t.assert_equals(err2, nil)

    local input3 = nil
    local res3, err3 = validate_utils.check_options_for_type({ input3 }, "string")
    t.assert_equals(res3, true)
    t.assert_equals(err3, nil)
end

g2.test_check_invalid_input = function()
    local input1 = {
        ["enable.auto.offset.store"] = true,
        ["group.id"] = "example_consumer",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
    }

    local res, err = validate_utils.check_options_for_type({ input1 }, "string")
    t.assert_equals(res, false)
    t.assert_str_contains(err, "ADG_KAFKA_CONNECTOR_VALIDATION_011")

    local input2 = {
        [true] = "true",
        ["group.id"] = "example_consumer",
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
    }

    local res2, err2 = validate_utils.check_options_for_type({ input2 }, "string")
    t.assert_equals(res2, false)
    t.assert_str_contains(err2, "ADG_KAFKA_CONNECTOR_VALIDATION_011")

    local input3 = {
        ["enable.auto.offset.store"] = "true",
        ["group.id"] = 123,
        ["auto.offset.reset"] = "earliest",
        ["enable.partition.eof"] = "false",
    }

    local res3, err3 = validate_utils.check_options_for_type({ input3 }, "string")
    t.assert_equals(res3, false)
    t.assert_str_contains(err3, "ADG_KAFKA_CONNECTOR_VALIDATION_011")
end

g3.test_check_valid_input = function()
    local input1 = { ["host"] = "localhost", ["port"] = 8081 }
    local res, err = validate_utils.check_schema_registry_opts(input1)
    t.assert_equals(res, true)
    t.assert_equals(err, nil)
end

g3.test_check_invalid_input = function()
    local input1 = nil
    local res, err = validate_utils.check_schema_registry_opts(input1)
    t.assert_equals(res, false)
    t.assert_str_contains(err, "SCHEMA_REGISTRY_VALIDATION_001")

    local input2 = "string"
    local res2, err2 = validate_utils.check_schema_registry_opts(input2)
    t.assert_equals(res2, false)
    t.assert_str_contains(err2, "SCHEMA_REGISTRY_VALIDATION_002")

    local input3 = {}
    local res3, err3 = validate_utils.check_schema_registry_opts(input3)
    t.assert_equals(res3, false)
    t.assert_str_contains(err3, "SCHEMA_REGISTRY_VALIDATION_003")

    local input4 = { ["host"] = "localhost" }
    local res4, err4 = validate_utils.check_schema_registry_opts(input4)
    t.assert_equals(res4, false)
    t.assert_str_contains(err4, "SCHEMA_REGISTRY_VALIDATION_005")

    local input5 = { ["port"] = 8081 }
    local res5, err5 = validate_utils.check_schema_registry_opts(input5)
    t.assert_equals(res5, false)
    t.assert_str_contains(err5, "SCHEMA_REGISTRY_VALIDATION_003")

    local input6 = { ["host"] = 123, ["port"] = 8081 }
    local res6, err6 = validate_utils.check_schema_registry_opts(input6)
    t.assert_equals(res6, false)
    t.assert_str_contains(err6, "SCHEMA_REGISTRY_VALIDATION_004")

    local input7 = { ["host"] = "localhost", ["port"] = "abc" }
    local res7, err7 = validate_utils.check_schema_registry_opts(input7)
    t.assert_equals(res7, false)
    t.assert_str_contains(err7, "SCHEMA_REGISTRY_VALIDATION_006")
end
