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

local fio = require("fio")

local t = require("luatest")
local json = require("json")
local g_memtx = t.group("adg_api_memtx")
local g_vinyl = t.group("adg_api_vinyl")

local file_utils = require("app.utils.file_utils")

local helpers = require("tnt_test.helper")

math.randomseed(os.time())

local function before_all(g, engine)
    g.cluster = helpers.Cluster:new({
        datadir = fio.tempdir(),
        server_command = helpers.entrypoint("adg_api_test_init"),
        use_vshard = true,
        replicasets = {
            {
                uuid = helpers.uuid("a"),
                alias = "adg_api",
                roles = { "app.roles.adg_api" },
                servers = {
                    { instance_uuid = helpers.uuid("a", 1), alias = "adg_api" },
                },
            },
            {
                uuid = helpers.uuid("b"),
                alias = "adg_storage",
                roles = { "app.roles.adg_storage" },
                servers = {
                    { instance_uuid = helpers.uuid("b", 1), alias = "adg_storage-master" },
                    { instance_uuid = helpers.uuid("b", 2), alias = "adg_storage-replica" },
                },
            },
            {
                uuid = helpers.uuid("c"),
                alias = "adg_kafka_connector",
                roles = { "app.roles.adg_kafka_connector" },
                servers = {
                    { instance_uuid = helpers.uuid("c", 1), alias = "adg_kafka_connector" },
                },
            },
            {
                uuid = helpers.uuid("d"),
                alias = "adg_scheduler",
                roles = { "app.roles.adg_scheduler" },
                servers = {
                    { instance_uuid = helpers.uuid("d", 1), alias = "adg_scheduler" },
                },
            },
            {
                uuid = helpers.uuid("e"),
                alias = "adg_input_processor",
                roles = { "app.roles.adg_input_processor" },
                servers = {
                    { instance_uuid = helpers.uuid("e", 1), alias = "adg_input_processor" },
                },
            },
            {
                uuid = helpers.uuid("f"),
                alias = "adg_output_processor",
                roles = { "app.roles.adg_output_processor" },
                servers = {
                    { instance_uuid = helpers.uuid("f", 1), alias = "adg_output_processor" },
                },
            },
            {
                uuid = helpers.uuid("1"),
                alias = "adg_state",
                roles = { "app.roles.adg_state" },
                servers = {
                    { instance_uuid = helpers.uuid("1", 1), alias = "adg_state" },
                },
            },
        },
        env = {
            ["ENGINE"] = engine,
        },
    })
    g.engine = engine
    g.cluster:start()
end

local function after_all(g)
    g.cluster:stop()
    fio.rmtree(g.cluster.datadir)
end

local function before_each(g)
    for _, server in ipairs(g.cluster.servers) do
        server.net_box:eval([[
            local space = box.space.customers
            if space ~= nil and not box.cfg.read_only then
                space:truncate()
            end
        ]])
    end
end

g_memtx.before_all = function()
    before_all(g_memtx, "memtx")
end
g_vinyl.before_all = function()
    before_all(g_vinyl, "vinyl")
end

g_memtx.after_all = function()
    after_all(g_memtx)
end
g_vinyl.after_all = function()
    after_all(g_vinyl)
end

g_memtx.before_each(function()
    before_each(g_memtx)
end)
g_vinyl.before_each(function()
    before_each(g_vinyl)
end)

local function add(name, fn)
    g_memtx[name] = fn
    g_vinyl[name] = fn
end

local function check_future_error(err, expected_errorCode, expected_opts_error)
    expected_errorCode = expected_errorCode or "STORAGE_003"
    expected_opts_error = expected_opts_error or "fiber is cancelled"
    t.assert(err)
    err = json.decode(err)
    t.assert_equals(err.status, "error")
    t.assert_equals(err.errorCode, expected_errorCode)
    t.assert_equals(err.opts.error, expected_opts_error)
end

add("test_futures_errors", function(g)
    local conn = g.cluster:server("adg_api").net_box
    local schema = file_utils.read_file("tnt_test/integration/data/schema_ddl.yml")
    local res, err = conn:call("set_ddl", { schema })
    t.assert_equals(err, nil)

    res, err = conn:call("transfer_data_to_scd_table_on_cluster", { "", "", "", 1 })
    check_future_error(err)

    res, err = conn:call("reverse_history_in_scd_table_on_cluster", { "", "", "", 1, 1 })
    check_future_error(err)

    res, err = conn:call("execute_query_for_massive_select_to_kafka", { "", "select ID from USER1" })
    check_future_error(err, "ADG_OUTPUT_PROCESSOR_001")

    --TODO load_avro_lines function unused in code
    --res, err = conn:call('load_avro_lines', { 'USER1', { 1, 2 } })
    --err = json.decode(err)
    --t.assert_equals(res, false)
    --t.assert_equals(err.status, "error")
    --t.assert_equals(err.errorCode, "STORAGE_003")
    --t.assert_equals(err.opts.error, "fiber is cancelled")
end)
