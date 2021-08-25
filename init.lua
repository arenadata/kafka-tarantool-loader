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

require("strict").on()

if package.setsearchroot ~= nil then
    package.setsearchroot()
else
    -- Workaround for rocks loading in tarantool 1.10
    -- It can be removed in tarantool > 2.2
    -- By default, when you do require('mymodule'), tarantool looks into
    -- the current working directory and whatever is specified in
    -- package.path and package.cpath. If you run your app while in the
    -- root directory of that app, everything goes fine, but if you try to
    -- start your app with "tarantool myapp/init.lua", it will fail to load
    -- its modules, and modules from myapp/.rocks.
    local fio = require("fio")
    local app_dir = fio.abspath(fio.dirname(arg[0]))
    print("App dir set to " .. app_dir)
    package.path = package.path .. ";" .. app_dir .. "/?.lua"
    package.path = package.path .. ";" .. app_dir .. "/?/init.lua"
    package.path = package.path .. ";" .. app_dir .. "/.rocks/share/tarantool/?.lua"
    package.path = package.path .. ";" .. app_dir .. "/.rocks/share/tarantool/?/init.lua"
    package.cpath = package.cpath .. ";" .. app_dir .. "/?.so"
    package.cpath = package.cpath .. ";" .. app_dir .. "/?.dylib"
    package.cpath = package.cpath .. ";" .. app_dir .. "/.rocks/lib/tarantool/?.so"
    package.cpath = package.cpath .. ";" .. app_dir .. "/.rocks/lib/tarantool/?.dylib"
end

local cartridge = require("cartridge")
local ok, err = cartridge.cfg({
    workdir = "tmp/db",
    roles = {
        "cartridge.roles.vshard-storage",
        "cartridge.roles.vshard-router",
        "app.roles.adg_api",
        "app.roles.adg_storage",
        "app.roles.adg_kafka_connector",
        "app.roles.adg_scheduler",
        "app.roles.adg_input_processor",
        "app.roles.adg_output_processor",
        "app.roles.adg_state",
    },
    cluster_cookie = "memstorage-cluster-cookie",
})

assert(ok, tostring(err))
