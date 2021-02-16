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

local log = require('log')
local checks = require('checks')

local function print_table(node)
    
    if(type(node)) == 'string' then
        log.error(node)
    else

    local cache, stack, output = {},{},{}
    local depth = 1
    local output_str = "{\n"

    while true do
        local size = 0
        for _,_ in pairs(node) do
            size = size + 1
        end

        local cur_index = 1
        for k,v in pairs(node) do
            if (cache[node] == nil) or (cur_index >= cache[node]) then

                if (string.find(output_str,"}",output_str:len())) then
                    output_str = output_str .. ",\n"
                elseif not (string.find(output_str,"\n",output_str:len())) then
                    output_str = output_str .. "\n"
                end

                -- This is necessary for working with HUGE
                -- tables otherwise we run out of memory using concat on huge strings
                table.insert(output,output_str)
                output_str = ""

                local key
                if (type(k) == "number" or type(k) == "boolean") then
                    key = "["..tostring(k).."]"
                else
                    key = "['"..tostring(k).."']"
                end

                if (type(v) == "number" or type(v) == "boolean") then
                    output_str = output_str .. string.rep('\t',depth) .. key .. " = "..tostring(v)
                elseif (type(v) == "table") then
                    output_str = output_str .. string.rep('\t',depth) .. key .. " = {\n"
                    table.insert(stack,node)
                    table.insert(stack,v)
                    cache[node] = cur_index+1
                    break
                else
                    output_str = output_str .. string.rep('\t',depth) .. key .. " = '"..tostring(v).."'"
                end

                if (cur_index == size) then
                    output_str = output_str .. "\n" .. string.rep('\t',depth-1) .. "}"
                else
                    output_str = output_str .. ","
                end
            else
                -- close the table
                if (cur_index == size) then
                    output_str = output_str .. "\n" .. string.rep('\t',depth-1) .. "}"
                end
            end

            cur_index = cur_index + 1
        end

        if (size == 0) then
            output_str = output_str .. "\n" .. string.rep('\t',depth-1) .. "}"
        end

        if (#stack > 0) then
            node = stack[#stack]
            stack[#stack] = nil
            depth = cache[node] == nil and depth + 1 or depth - 1
        else
            break
        end
    end

    -- This is necessary for working with HUGE tables otherwise we run out of memory using concat on huge strings
    table.insert(output,output_str)
    output_str = table.concat(output)

    log.error(output_str)
end
end


local function split_table_in_chunks(data, number_of_chunks)
    checks('table','number')
    local result = {}
    for  k,v in ipairs(data) do
        local bucket = math.fmod(k,number_of_chunks) + 1
        if result[bucket] == nil then
            result[bucket] = {v}
        else table.insert(result[bucket],v)
        end
    end
    return result
end

local function generate_limit_offset(all_rows_cnt, chunk_size)
    checks('number', 'number')
    local result = {}

    for i=1,math.ceil(all_rows_cnt/chunk_size),1 do
        local offset = (i - 1) * chunk_size
        local limit = 0

        if i * chunk_size > all_rows_cnt then
            limit =  all_rows_cnt - (i - 1) * chunk_size
        else
            limit = chunk_size
        end

        result[i] = {limit = limit , offset = offset}
    end

    return result
end

local function memoize(f)
    local mem = {}
    setmetatable(mem, {__mode = "kv"})
    return function (x)
        local r = mem[x]
        if r == nil then
            r = f(x)
            mem[x] = r
        end
        return r
    end
end

local function table_length(T)
    local count = 0
    for _ in pairs(T) do count = count + 1 end
    return count
  end

local function string_last_index_of(haystack, needle)
    local i, j
    local k = 0
    repeat
        i = j
        j, k = string.find(haystack, needle, k + 1, true)
    until j == nil

    return i
end

local function append_table(where, from)
    for _, item in pairs(from) do
        table.insert(where, item)
    end
    return where
end

local function append_array(where, from)
    for _, item in ipairs(from) do
        table.insert(where, item)
    end
    return where
end

local function flatten(arr)
    local result = { }

    local function flatten(arr)
        for _, v in ipairs(arr) do
            if type(v) == "table" then
                flatten(v)
            else
                table.insert(result, v)
            end
        end
    end

    flatten(arr)
    return result
end

local function is_array(table)
    if type(table) ~= 'table' then
        return false
    end

    if #table > 0 then
        return true
    end

    for _,_ in pairs(table) do
        return false
    end

    return true
end


return {
    print_table = print_table,
    split_table_in_chunks = split_table_in_chunks,
    table_length = table_length,
    generate_limit_offset = generate_limit_offset,
    string_last_index_of = string_last_index_of,
    append_table = append_table,
    append_array = append_array,
    flatten = flatten,
    is_array = is_array
}