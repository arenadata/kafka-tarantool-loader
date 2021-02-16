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
--- Created by ashitov.
--- DateTime: 6/29/20 6:14 PM
---
local t = require('luatest')
local g = t.group('space_delete_batch_test')
local helper = require('test.helper.integration')
local cluster = helper.cluster

g.before_each(function()
    local state = cluster:server('state-1').net_box
    state:call('box.execute', {'truncate table _DELETE_SPACE_BATCH'})
end)

g.test_init_space = function ()
    local state = cluster:server('state-1').net_box
    t.assert_equals(type(state.space._DELETE_SPACE_BATCH), 'table')
end

g.test_update_delete_batch_storage = function()
    local state = cluster:server('state-1').net_box

    local res,err = state:call('update_delete_batch_storage', {'batch1',{'test2','test3'}})
    local table_data = state:call('get_tables_from_delete_batch',{'batch1'})
    t.assert_equals(res,true)
    t.assert_equals(err,nil)
    t.assert_equals(table_data,{'test2','test3'})


    --Add to existing array
    local res2,err2 = state:call('update_delete_batch_storage', {'batch1',{'test1','test4'}})
    local table_data2 = state:call('get_tables_from_delete_batch',{'batch1'})

    t.assert_equals(res2,true)
    t.assert_equals(err2,nil)
    t.assert_items_include(table_data2,{'test1','test2','test3','test4'})


    -- Add same items
    local res3,err3 = state:call('update_delete_batch_storage', {'batch1',{'test1','test4','test5'}})
    local table_data3 = state:call('get_tables_from_delete_batch',{'batch1'})

    t.assert_equals(res3,true)
    t.assert_equals(err3,nil)
    t.assert_equals(#table_data3,5)
    t.assert_items_include(table_data3,{'test1','test2','test3','test4','test5'})

    -- Add empty array

    local res4,err4 = state:call('update_delete_batch_storage', {'batch1',{}})
    local table_data4 = state:call('get_tables_from_delete_batch',{'batch1'})

    t.assert_equals(res4,true)
    t.assert_equals(err4,nil)
    t.assert_equals(#table_data4,5)
    t.assert_items_include(table_data4,{'test1','test2','test3','test4','test5'})
end

g.test_update_delete_batch_storage_not_stings = function()
    local state = cluster:server('state-1').net_box

    local res,err = state:call('update_delete_batch_storage', {'batch1',{1345,123,true,{}}})
    local table_data = state:call('get_tables_from_delete_batch',{'batch1'})
    t.assert_equals(res,true)
    t.assert_equals(err,nil)
    t.assert_equals(table_data,{})

    local res2,err2 = state:call('update_delete_batch_storage', {'batch1',{'test1','test4',666,{},false}})
    local table_data2 = state:call('get_tables_from_delete_batch',{'batch1'})

    t.assert_equals(res2,true)
    t.assert_equals(err2,nil)
    t.assert_items_include(table_data2,{'test1','test4'})

end

g.test_remove_delete_batch = function()
    local state = cluster:server('state-1').net_box

    local res,err = state:call('update_delete_batch_storage', {'batch1',{'test2','test3'}})
    local table_data = state:call('get_tables_from_delete_batch',{'batch1'})
    t.assert_equals(res,true)
    t.assert_equals(err,nil)
    t.assert_equals(table_data,{'test2','test3'})

    local res2,err2 = state:call('remove_delete_batch', {'batch1'})
    t.assert_equals(res2,true)
    t.assert_equals(err2,nil)
    local table_data2 = state:call('get_tables_from_delete_batch',{'batch1'})
    t.assert_equals(table_data2,{})
end