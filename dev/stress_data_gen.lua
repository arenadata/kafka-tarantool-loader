#!/usr/bin/env tarantool

local nb = require("net.box")
local fiber = require("fiber")
local http_client = require('http.client')
local log = require('log')
local json = require("json")

local body = {
    spaces = {
        VEHICLE = {
            format = {
                {
                    name = "id",
                    type = "integer",
                    is_nullable = false,
                },
                {
                    name = "gov_number",
                    type = "string",
                    is_nullable = false,
                },
                {
                    name = "sysOp",
                    type = "number",
                    is_nullable = false,
                },
                {
                    name = "bucket_id",
                    type = "unsigned",
                    is_nullable = true,
                },
            },
            temporary = false,
            engine = "vinyl",
            indexes = {
                {
                    unique = true,
                    parts = {
                        {
                            path = "id",
                            type = "integer",
                            is_nullable = false,
                        }
                    },
                    type = "TREE",
                    name = "id",
                },
                {
                    unique = false,
                    parts = {
                        {
                            path = "bucket_id",
                            type = "unsigned",
                            is_nullable = true,
                        },
                    },
                    type = "TREE",
                    name = "bucket_id",
                },
            },
            is_local = false,
            sharding_key = { "id" },
        },
    }
}

local response = http_client.post(
        'http://localhost:8081/api/v1/ddl/table/queuedCreate',
        json.encode(body),
        {
            {
                ["content-type"] = "application/json; charset=utf-8"
            }
        }
)

if response.status ~= 200 then
    log.info(response.body)
    os.exit(1)
end

log.info("table was created")
fiber.sleep(5)
log.info("loading data was started")

local api = nb.connect("admin:memstorage-cluster-cookie@localhost:3301")

for i = 1, 30000, 1 do
    local res, err = api:call("insert_record", { "VEHICLE", { i, "a777a750", 0 } })
    if res == nil then
        log.info(err)
        os.exit(1)
    end
end

api:close()

log.info("data was loaded")
fiber.sleep(5)

local storage_ports = {
    "3304",
    "3308"
}

for i, port in pairs(storage_ports) do
    local storage = nb.connect("admin:memstorage-cluster-cookie@localhost:" .. port)
    local rec_count, err = storage:eval("return box.space.VEHICLE:count();")
    if err ~= nil then
        log.info(err)
        os.exit(1)
    end
    storage:close()

    log.info("Storage %d has %d records", i, rec_count)

end
