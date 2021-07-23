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


local errors = require('errors')
local errno = require('errno')
local fio = require('fio')
local string = require('string')
local checks = require('checks')

local read_file_error_open = errors.new_class('read_file_err_open')
local read_file_error_read = errors.new_class('read_file_error_read')
local write_file_error_open = errors.new_class('write_file_error_open')
local write_file_error_write = errors.new_class('write_file_error_write')
local delete_file_error_delete = errors.new_class('delete_file_error_delete')

---read_file - method, that read file on host.
---@param path string - path to file.
---@return string - string, that contains file data.
local function read_file(path)
    checks('string')
    if path == nil then
        return nil, read_file_error_open:new('Failed to open file %s', path)
    end
    local file = fio.open(path,"O_RDONLY")
    if file == nil then
        return nil, read_file_error_open:new('Failed to open file %s: %s', path, errno.strerror())
    end
    local buf = {}
    while true do
        local val = file:read(1024)
        if val == nil then
            return nil, read_file_error_read:new('Failed to read from file %s: %s', path, errno.strerror())
        elseif val == '' then
            break
        end
        table.insert(buf, val)
    end
    file:close()
    return table.concat(buf, '')
end

---get_files_in_directory - method, that returns files in the directory with a mask.
---@param path string - path to the directory.
---@param mask string - mask to filter files.
---@return table - array with file paths.
local function get_files_in_directory(path,mask)
    checks('string','string')
    local file_list
    local file_separator = package.config:sub(1,1)

    if not fio.path.is_dir(path) then
        return {}
    end

    if string.sub(path,-1) == file_separator then file_list =  fio.glob(path .. mask)
    else file_list =  fio.glob(path .. file_separator .. mask)
    end
    if file_list == nil then return {}
    else return file_list
    end
end

---write_file - method, that write data to file on host.
---@param path string - path to file.
---@param entry string - data to write.
---@param flags table - list of string constants, see - https://man7.org/linux/man-pages/man2/open.2.html.
---@param mode table - list of string constants, see - https://man7.org/linux/man-pages/man2/open.2.html.
---@return boolean|string - result of operation | error message.
local function write_file(path,entry,flags,mode)
    checks('string','string','table','table')
    if fio.path.exists(path) then
        return false,write_file_error_open:new('Failed to open file, file exists %s', path)
    end

    local file = fio.open(path,flags,mode)
    if file == nil then
        return false, write_file_error_open:new('Failed to open file %s: %s', path, errno.strerror())
    end

    local write_result = file:write(entry)

    if not write_result then
        file:close()
        return false, write_file_error_write:new('Failed to write to file %s: %s', path, errno.strerror())
    end

    file:close()
    return true,nil
end

---delete_file - method, that delete file on host.
---@param path string - path to file.
---@return boolean|string - result of operation | error message.
local function delete_file(path)
    checks('string')

    if fio.path.exists(path) then
        local result = fio.unlink(path)
        if not result then
            return false, delete_file_error_delete:new('Failed to delete file %s: %s', path, errno.strerror())
        end
    end

    return true,nil

end


return {
    read_file = read_file,
    write_file = write_file,
    delete_file = delete_file,
    get_files_in_directory = get_files_in_directory
}
