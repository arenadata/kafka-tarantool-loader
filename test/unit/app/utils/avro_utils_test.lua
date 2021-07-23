#!/usr/bin/env tarantool
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

local t = require('luatest')


local shared = require('test.helper')
local avro_utils = require('app.utils.avro_utils')
local avro_schema = require('avro_schema')
local json = require("json")

local g = t.group('avro_utils.validate_avro_data')
local g2 = t.group('avro_utils.check_avro_schema_compatibility')
local g3 = t.group('avro_utils.compile_avro_schema')



local ok,nsud_schema_key = avro_schema.create(json.decode([[
    {
        "name": "AdbUploadRequest",

        "type": "record",

        "namespace": "ru.ibs.dtm.common.model",

        "fields": [


      {       "name": "requestId",       "type": "string"}

        ]

}
]]))

local nsud_schema_key_values = {json.decode('{"requestId": "14234-234234-234234-234234"}')}

local ok,nsud_schema_msg = avro_schema.create(json.decode([[
    {

        "name": "Employees",

        "type": "array",

        "namespace": "ru.ibs.dtm.common.model",

        "items": {

          "name": "Employees_record",

          "type": "record",

          "fields": [


      {         "name": "id",         "type": "int"}

      ,


      {         "name": "sysFrom",         "type": "int"}

      ,


      {         "name": "reqId",         "type": "int"}

      ,


      {         "name": "sysOp",         "type": "int"}

      ,


      {         "name": "name",         "type": "string"}

      ,


      {         "name": "department",         "type": "string"}

      ,


      {         "name": "manager",         "type": "string"}

      ,


      {         "name": "salary",         "type": "int"}

          ]

}

}
]]))

-- luacheck: max line length 150
 local nsud_schema_msg_values = json.decode([[
    [
        {"id": 1,   "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Robin Hood",    "department": "",    "manager": "",            "salary": 200}
        ,
        {"id": 2,   "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Arsene Wenger", "department": "Bar", "manager": "Friar Tuck",  "salary": 50}
        ,
        {"id": 33,  "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Friar Tuck",    "department": "Foo", "manager": "Robin Hood",  "salary": 100}
        ,
        {"id": 44,  "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Little John",   "department": "Foo", "manager": "Robin Hood",  "salary": 100}
        ,
        {"id": 125, "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Sam Allardyce", "department": "",    "manager": "",            "salary": 250}
        ,
        {"id": 555, "sysFrom": 10, "reqId": 155, "sysOp": 0, "name": "Dimi Berbatov", "department": "Foo", "manager": "Little John", "salary": 50}
        ]
]])

g.test_check_avro_schema_object = function ()
    local schema = {}
    local data = {}
    local ok,err = avro_utils.validate_avro_data(schema,data)
    t.assert_equals(ok,false)
    t.assert_equals(err,nil)

    local nil_schema = nil
    t.assert_equals(avro_utils.validate_avro_data(nil_schema,data), false)
end

g.test_valid_data = function()
    local ok,test_schema = avro_schema.create ({
        type = "record",
        name = "Frob",
        fields = {
          { name = "foo", type = "int", default = 42},
          { name = "bar", type = "string"},
          { name = 'kek', type = 'string'}
}
})


    local test_data = {foo = 1, bar = 'test', kek = 'test2'}

    local ok,err = avro_utils.validate_avro_data(test_schema,test_data)
    t.assert_equals(ok,true)



    local ok, err = avro_utils.validate_avro_data(nsud_schema_key,nsud_schema_key_values[1])
    t.assert_equals(ok,true)



    local ok,err = avro_utils.validate_avro_data(nsud_schema_msg,nsud_schema_msg_values)
    t.assert_equals(ok,true)


end

g.test_invalid_data = function ()

    local ok,test_schema = avro_schema.create ({
        type = "record",
        name = "Frob",
        fields = {
          { name = "foo", type = "int", default = 42},
          { name = "bar", type = "string"},
          { name = 'kek', type = 'string'}
}
})


    local test_data1 = {
        [1] = {foo = 1, bar = 'test', kek = 'test2'},
        [2] = {foo = '2', bar = 'test', kek = 'test2'},
        [3] = {foo = 3, bar = 'test', kek = 'test2'}
}

    local ok,err = avro_utils.validate_avro_data(test_schema,test_data1)
    t.assert_equals(ok,false)



    local test_data2 = {
        [1] = {foo = 1, bar = 'test', kek = 'test2'},
        [2] = {foo = '2', bar = 'test', kek = 'test2'},
        [3] = {foo = 3, bar = 'test', kek = 'test2'}
}

    local ok,err = avro_utils.validate_avro_data(test_schema,test_data2)
    t.assert_equals(ok,false)




    local test_data3 = {
        [1] = {foo = 1, bar = 'test', kek = 'test2'},
        [2] = {foo = '2', bar = 'test', kek = 'test2'},
        [3] = {foo = 3, bar2 = 'test', kek = 'test2'}
}

    local ok,err = avro_utils.validate_avro_data(test_schema,test_data3)
    t.assert_equals(ok,false)



    local empty_data = {
}

    local ok,err = avro_utils.validate_avro_data(test_schema,empty_data)
    t.assert_equals(ok,false)
    t.assert_equals(err,'Field bar missing')

    local nil_data = nil




end


g2.test_input_check = function ()
    local nil_schema= nil
    local empty_table = {}
    local err_opts = {err = '123'}
    local valid_opts_false = {is_downgrade=false}
    local valid_opts_true = {is_downgrade=true}
    local ok,valid_schema = avro_schema.create ({
        type = "record",
        name = "Frob",
        fields = {
          { name = "foo", type = "int", default = 42},
          { name = "bar", type = "string"},
          { name = 'kek', type = 'string'}
}
})


end

g2.test_forward_check = function ()

end

g2.test_backward_check = function ()

end


g3.test_input_check = function ()
    local nil_schema = nil
    local empty_table = {}
    local ok,valid_schema = avro_schema.create ({
        type = "record",
        name = "Frob",
        fields = {
          { name = "foo", type = "int", default = 42},
          { name = "bar", type = "string"},
          { name = 'kek', type = 'string'}
}
})

      t.assert_error(avro_utils.compile_avro_schema,nil_schema)

      local ok,methods = avro_utils.compile_avro_schema(empty_table)
      t.assert_equals(ok,false)
      t.assert_equals(methods,nil)

      local ok,methods = avro_utils.compile_avro_schema(valid_schema)
      t.assert_equals(ok,true)
end

g3.test_simple_compile_check = function ()
    local ok,valid_schema = avro_schema.create ({
        type = "record",
        name = "Frob",
        fields = {
          { name = "foo", type = "int", default = 42},
          { name = "bar", type = "string"},
          { name = 'kek', type = 'string'}
}
})
      local ok,methods = avro_utils.compile_avro_schema(valid_schema)
      local valid_methods = {
        flatten,
        unflatten,
        xflatten,
        flatten_msgpack,
        unflatten_msgpack,
        xflatten_msgpack,
        get_types,
        get_names,
}

      t.assert_items_include(methods,valid_methods)
end