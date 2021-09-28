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
--- Created by Igor Kuznetsov.
--- DateTime: 5/10/21 12:13
---
local t = require("luatest")
local g1 = t.group("new_query")

local helper = require("test.helper.integration")
local cluster = helper.cluster

g1.before_each(
        function()
            local api = cluster:server("api-1").net_box

            local r, err = api:call("insert_record", { "EMPLOYEES_TRANSFER", { 1, 1, 1, 1, "123", "123", "123", 100 } })
            t.assert_equals(err, nil)
            t.assert_equals(r, 1)

            r, err = api:call("insert_record", { "EMPLOYEES_TRANSFER_HIST", { 1, 1, 1, 1, 1, "123", "1000", "1000", 5000 } })
            t.assert_equals(err, nil)
            t.assert_equals(r, 1)

        end
)

g1.after_each(
        function()
            local storage1 = cluster:server("master-1-1").net_box
            local storage2 = cluster:server("master-2-1").net_box
            storage1:call("box.execute", { "truncate table EMPLOYEES_TRANSFER" })
            storage2:call("box.execute", { "truncate table EMPLOYEES_TRANSFER_HIST" })
        end
)

g1.test_incorrect_query = function()
    local api = cluster:server("api-1").net_box

    local _, err = api:call("query", { [[SELECT * FROM EMPLOYEES_TRANSFER as "a"
            INNER JOIN EMPLOYEES_HIST as "b" ON "a"."id" = "b"."a_id"
        WHERE "id" = 5 and "name" = '123']] })
    t.assert_equals(err, "query wasn't s implemented")
end

g1.test_simple_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM EMPLOYEES_TRANSFER where "id" = 5 and "name" = '123']] })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
            {name = "sysFrom", type = "number"},
            {name = "reqId", type = "number"},
            {name = "sysOp", type = "number"},
            {name = "name", type = "string"},
            {name = "department", type = "string"},
            {name = "manager", type = "string"},
            {name = "salary", type = "number"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {},
    })

    r, err = api:call("query", { [[SELECT * FROM EMPLOYEES_TRANSFER where "id" = 1]] })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            {name = "id", type = "number"},
            {name = "sysFrom", type = "number"},
            {name = "reqId", type = "number"},
            {name = "sysOp", type = "number"},
            {name = "name", type = "string"},
            {name = "department", type = "string"},
            {name = "manager", type = "string"},
            {name = "salary", type = "number"},
            {name = "bucket_id", type = "unsigned"},
        },
        rows = {
            { 1, 1, 1, 1, "123", "123", "123", 100, 3939 }
        },
    })

    r, err = api:call("query", { [[SELECT * FROM (
            SELECT "id", "name", "department", "manager", "salary", "bucket_id" FROM EMPLOYEES_TRANSFER WHERE "sysOp" > 0
            UNION ALL
            SELECT "id", "name", "department", "manager", "salary", "bucket_id" FROM EMPLOYEES_TRANSFER_HIST WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1]] })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            { name = "id", type = "number" },
            { name = "name", type = "string" },
            { name = "department", type = "string" },
            { name = "manager", type = "string" },
            { name = "salary", type = "number" },
            { name = "bucket_id", type = "unsigned" },
        },
        rows = {
            { 1, "123", "123", "123", 100, 3939 },
            { 1, "123", "1000", "1000", 5000, 3939 }
        },
    })

end

g1.test_union_query = function()
    local api = cluster:server("api-1").net_box

    local r, err = api:call("query", { [[SELECT * FROM (
            SELECT "id", "name", "department", "manager", "salary", "bucket_id" FROM EMPLOYEES_TRANSFER WHERE "sysOp" > 0
            UNION ALL
            SELECT "id", "name", "department", "manager", "salary", "bucket_id" FROM EMPLOYEES_TRANSFER_HIST WHERE "sysOp" > 0
        ) as "t1"
        WHERE "id" = 1]] })
    t.assert_equals(err, nil)
    t.assert_equals(r, {
        metadata = {
            { name = "id", type = "number" },
            { name = "name", type = "string" },
            { name = "department", type = "string" },
            { name = "manager", type = "string" },
            { name = "salary", type = "number" },
            { name = "bucket_id", type = "unsigned" },
        },
        rows = {
            { 1, "123", "123", "123", 100, 3939 },
            { 1, "123", "1000", "1000", 5000, 3939 }
        },
    })

end