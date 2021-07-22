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


local fiber = require('fiber')

-- class which implement map of mutex table
local mutex_map = {}

function mutex_map:new()
    local cache= {}

    -- method lock space for writing
    --- @param table string - space for lock
    function cache:lock(table)
        if self[table] == nil then
            self[table] = fiber.channel(1)
        end
        self[table]:put(true)
    end

    -- method unlock space for writing
    --- @param table string - space for unlock
    function cache:unlock(table)
        if self[table] ~= nil then
            self[table]:get()
            self[table] = nil
        end
    end

    -- method clear lock tables
    function cache:clear()
        self = {}
    end

    setmetatable(cache, self)
    self.__index = self

    return cache
end

function init_mutex_map()
    return mutex_map:new()
end

return {
    init_mutex_map = init_mutex_map
}