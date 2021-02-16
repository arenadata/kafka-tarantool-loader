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
------ Created by ashitov.
--- DateTime: 4/17/20 1:15 PM
---
local file_utils = require('app.utils.file_utils')
local t = require('luatest')

local g = t.group('file_utils.read_file')
local g2 = t.group('file_utils.get_files_in_directory')
local g3 = t.group('file_utils.write_file')
local g4 = t.group('file_utils.delete_file')

g3.before_each(function()
    local files = file_utils.get_files_in_directory('/tmp/','test_file_*.tarantool_test')
    for _,file in ipairs(files) do
        file_utils.delete_file(file)
    end
end)

local function generate_random_txt_file_name()
    local random_number
    local random_string
    random_string = ""
    for _ = 1,10,1 do
        random_number = math.random(65, 90)
        random_string = random_string .. string.char(random_number)
    end
    return 'test_file_' .. random_string .. '.tarantool_test'
end

g.test_read_simple_files = function ()

    local line,err = file_utils.read_file('test/unit/data/simple_files/singleline.txt')
    t.assert_equals(line,'Hello World!')
    t.assert_equals(err,nil)

    local line2,err2 = file_utils.read_file('test/unit/data/simple_files/multiline.txt')
    t.assert_equals(line2,'Hello World1!\nHello World2!')
    t.assert_equals(err2,nil)

end

g.test_read_bad_path = function()
    local line,err = file_utils.read_file('test/unit/data/simple_files/singleline2.txt')
    t.assert_equals(line,nil)
    t.assert_str_contains(tostring(err),'singleline2.txt: No such file or directory')

    t.assert_error(file_utils.read_file,nil)
end

g2.test_scan_simple_directory = function ()
    local files,err = file_utils.get_files_in_directory('test/unit/data/simple_files/', '*.txt')
    t.assert_equals(files,{'test/unit/data/simple_files/multiline.txt','test/unit/data/simple_files/singleline.txt'})
    t.assert_equals(err,nil)

    local files2,err2 = file_utils.get_files_in_directory('test/unit/data/simple_files/', '*.yml')
    t.assert_equals(files2,{})
    t.assert_equals(err2,nil)
end

g2.test_wrong_args_input = function ()
    t.assert_error(file_utils.get_files_in_directory,nil,'*.txt')
    t.assert_error(file_utils.get_files_in_directory,'/ttt',nil)
    t.assert_error(file_utils.get_files_in_directory,nil,nil)
end

g3.test_wrong_args_input = function()
    t.assert_error(file_utils.write_file,nil,"123",{"O_RDWR"},{"S_IRUSR"})
    t.assert_error(file_utils.write_file,"/tmp/test_file1.txt",nil,{"O_RDWR"},{"S_IRUSR"})
    t.assert_error(file_utils.write_file,"/tmp/test_file1.txt","123",nil,{"S_IRUSR"})
    t.assert_error(file_utils.write_file,"/tmp/test_file1.txt","123",{"O_RDWR"},nil)
end

g3.test_simple_write = function()
    local one_string = "Hello World!"
    local two_string = 'Hello World1!\nHello World2!'

    local file_name1 = generate_random_txt_file_name()
    local file_name2 = generate_random_txt_file_name()

    local res1,err1 = file_utils.write_file("/tmp/" .. file_name1,one_string,{"O_RDWR","O_CREAT"},
            {"S_IRUSR", "S_IWUSR"})
    t.assert_equals(res1,true)
    t.assert_equals(err1,nil)


    local res2,err2 = file_utils.write_file("/tmp/" .. file_name2,two_string,{"O_RDWR","O_CREAT"},
            {"S_IRUSR", "S_IWUSR"})
    t.assert_equals(res2,true)
    t.assert_equals(err2,nil)

    local files = file_utils.get_files_in_directory('/tmp/','test_file_*.tarantool_test')

    t.assert_equals(#files,2)

    local line,err3 = file_utils.read_file("/tmp/" .. file_name1)
    t.assert_equals(line,'Hello World!')
    t.assert_equals(err3,nil)

    local line2,err4 = file_utils.read_file("/tmp/" .. file_name2)
    t.assert_equals(line2,'Hello World1!\nHello World2!')
    t.assert_equals(err4,nil)

    local del1,del_err1 = file_utils.delete_file("/tmp/" .. file_name1)
    local del2,del_err2 = file_utils.delete_file("/tmp/" .. file_name2)

    t.assert_equals(del1,true)
    t.assert_equals(del_err1,nil)

    t.assert_equals(del2,true)
    t.assert_equals(del_err2,nil)

    local files2 = file_utils.get_files_in_directory('/tmp/','test_file_*.tarantool_test')
    t.assert_equals(#files2,0)

end


g3.test_write_to_existing_file = function()
    local one_string = "Hello World!"
    local file_name1 = generate_random_txt_file_name()

    local res1,err1 = file_utils.write_file("/tmp/" .. file_name1,one_string,{"O_RDWR","O_CREAT"},
            {"S_IRUSR", "S_IWUSR"})
    t.assert_equals(res1,true)
    t.assert_equals(err1,nil)


    local res2,err2 = file_utils.write_file("/tmp/" .. file_name1,one_string,{"O_RDWR","O_CREAT"},
            {"S_IRUSR", "S_IWUSR"})
    t.assert_equals(res2,false)
    t.assert_str_contains(tostring(err2),'file exists')


    local del1,del_err1 = file_utils.delete_file("/tmp/" .. file_name1)


    t.assert_equals(del1,true)
    t.assert_equals(del_err1,nil)


    local files2 = file_utils.get_files_in_directory('/tmp/','test_file_*.tarantool_test')
    t.assert_equals(#files2,0)

end

g4.test_wrong_args_input = function()
    t.assert_error(file_utils.delete_file,nil)
    t.assert_error(file_utils.delete_file,1)
    t.assert_error(file_utils.delete_file,{"123"})
end

g4.test_delete_file = function()

    local one_string = "Hello World!"
    local file_name1 = generate_random_txt_file_name()

    local res1,err1 = file_utils.write_file("/tmp/" .. file_name1,one_string,{"O_RDWR","O_CREAT"},
            {"S_IRUSR", "S_IWUSR"})
    t.assert_equals(res1,true)
    t.assert_equals(err1,nil)
    local del1,del_err1 = file_utils.delete_file("/tmp/" .. file_name1)


    t.assert_equals(del1,true)
    t.assert_equals(del_err1,nil)


    local files2 = file_utils.get_files_in_directory('/tmp/','test_file_*.tarantool_test')
    t.assert_equals(#files2,0)

end