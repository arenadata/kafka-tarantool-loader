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

local t = require("luatest")

local log = require("log")
local checks = require("checks")
local digest = require("digest")
local fio = require("fio")

if os.getenv("DEV") == nil then
    os.setenv("DEV", "ON")
end

local ok, cartridge_helpers = pcall(require, "cartridge.test-helpers")
if not ok then
    log.error("Please, install cartridge rock to run tests")
    os.exit(1)
end

local helpers = table.copy(cartridge_helpers)

helpers.project_root = fio.dirname(debug.sourcedir())

local __fio_tempdir = fio.tempdir
fio.tempdir = function(base)
    base = base or os.getenv("TMPDIR")
    if base == nil or base == "/tmp" then
        return __fio_tempdir()
    else
        local random = digest.urandom(9)
        local suffix = digest.base64_encode(random, { urlsafe = true })
        local path = fio.pathjoin(base, "tmp.cartridge." .. suffix)
        fio.mktree(path)
        return path
    end
end

function helpers.entrypoint(name)
    local path = fio.pathjoin(helpers.project_root, "tnt_test", "entrypoint", string.format("%s.lua", name))
    if not fio.path.exists(path) then
        error(path .. ": no such entrypoint", 2)
    end
    return path
end

return helpers
