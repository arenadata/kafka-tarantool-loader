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
--- DateTime: 3/24/20 2:21 PM
---
local t = require('luatest')
local g = t.group('storage')
local g2 = t.group('storage_delta_processing_test')
local g3 = t.group('storage_drop_space')
local g4 = t.group('storage_delta_rollback')
local g5 = t.group('storage_delete_scd_sql')
local g6 = t.group('storage_delta_checksum')
local helper = require('test.helper.integration')
local cluster = helper.cluster

g2.before_each(function()
    local storage = cluster:server('master-1-1').net_box
    storage:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
end)

g4.before_each(function()
    local storage = cluster:server('master-1-1').net_box
    storage:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST_2'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_2'})
end)

g5.before_each(function ()
    local storage = cluster:server('master-1-1').net_box
    storage:call('box.execute', {'truncate table EMPLOYEES_HOT'})
end)

g6.before_each(function()
    local storage = cluster:server('master-1-1').net_box
    storage:call('box.execute', {'truncate table EMPLOYEES_HOT'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER_HIST'})
    storage:call('box.execute', {'truncate table EMPLOYEES_TRANSFER'})
end)


g.test_get_space_format = function ()
    local storage = cluster:server('master-1-1').net_box

    local res, _ = storage:call('get_space_format',{'NOT_EXISTS_SPACE'} )
    t.assert_equals(res, nil)

    local res2,err2 = storage:call('get_space_format',{'EMPLOYEES'} )
    t.assert_equals(err2, nil)
    t.assert_equals(res2, {"ID", "sysFrom", "FIRST_NAME", "LAST_NAME", "EMAIL", "sysOp", "bucket_id"})

end

g.test_check_table_for_delta_fields =  function ()
    local storage = cluster:server('master-1-1').net_box

    local res,err = storage:call('check_table_for_delta_fields',{'EMPLOYEES', 'actual'} )
    t.assert_equals(res, true)
    t.assert_equals(err, nil)

    local res2, _ = storage:call('check_table_for_delta_fields',{'EMPLOYEES_BAD1', 'actual'} )
    t.assert_equals(res2, false)

    local res3, _ = storage:call('check_table_for_delta_fields',{'EMPLOYEES_BAD2', 'actual'} )
    t.assert_equals(res3, false)

    local res4, _ = storage:call('check_table_for_delta_fields',{'NOT_EXISTS_SPACE', 'actual'} )
    t.assert_equals(res4, false)

    local res5, _ = storage:call('check_table_for_delta_fields',{'EMPLOYEES_BAD3', 'actual'} )
    t.assert_equals(res5, false)

    local res6,err6 = storage:call('check_table_for_delta_fields',{'EMPLOYEES_HIST', 'history'} )
    t.assert_equals(res6, true)
    t.assert_equals(err6, nil)

    local res7, _ = storage:call('check_table_for_delta_fields',{'EMPLOYEES_HIST_BAD', 'history'} )
    t.assert_equals(res7, false)
end

g.test_check_tables_for_delta = function ()
    local storage = cluster:server('master-1-1').net_box

    local res,err = storage:call('check_tables_for_delta',{'EMPLOYEES', 'EMPLOYEES_HIST'} )
    t.assert_equals(res, true)
    t.assert_equals(err, nil)

    local res2, _ = storage:call('check_tables_for_delta',{'EMPLOYEES', 'EMPLOYEES_HIST_BAD2'} )
    t.assert_equals(res2, false)
end


g2.test_1k_rows_transfer_data_scd = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1})

    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)


    datagen(1000)



    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local cnt2_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,1000)
    t.assert_equals(cnt2_3,1000)



end

g2.test_1k_rows_transfer_data_scd_with_changed_ids = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )

    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)


    datagen(2000)



    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local cnt2_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,2000)
    t.assert_equals(cnt2_3,1000)

end

g2.test_1k_rows_transfer_data_scd_with_broken_load = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)
    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',1} )

    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)

    datagen(1000)

    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',1} )

    local cnt2_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,1000)
    t.assert_equals(cnt2_3,0)


end



g2.test_100k_rows_transfer_data_scd = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(100000)

    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )

    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,100000)
    t.assert_equals(cnt1_3,0)


    datagen(100000)



    local res,err = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local cnt2_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,100000)
    t.assert_equals(cnt2_3,100000)



end


g3.test_existing_space_drop = function()
    local storage = cluster:server('master-1-1').net_box
    storage:call('box.execute', {'create table TEST_TABLE_FOR_DROP(ID INT PRIMARY KEY)'})
    t.assert_not_equals(storage.space.TEST_TABLE_FOR_DROP,nil)

    local res,err = storage:call('drop_space', {'TEST_TABLE_FOR_DROP'})
    t.assert_equals(storage.space.TEST_TABLE_FOR_DROP, nil)
    t.assert_equals(res,true)
    t.assert_equals(err,nil)
end

g3.test_not_existing_space_drop = function()
    local storage = cluster:server('master-1-1').net_box
    t.assert_equals(storage.space.NOT_EXISTS, nil)
    local res,err = storage:call('drop_space', {'NOT_EXISTS'})
    t.assert_equals(res,nil)
    t.assert_not_equals(err,nil)

    t.assert_error(storage.call,storage,'drop_space', {nil})
end

g4.test_1k_rows_rollback_w_index_wo_batch = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local actual_1 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_1 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2} )

    local res,err = storage:call('reverse_history_in_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2, nil} )
    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})
    local actual_2 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_2 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)
    t.assert_equals(actual_1,actual_2)
    t.assert_equals(history_1,history_2)

end


g4.test_1k_rows_rollback_w_index_w_batch = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local actual_1 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_1 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2} )

    local res,err = storage:call('reverse_history_in_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2, 100} )
    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})
    local actual_2 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_2 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)
    t.assert_equals(actual_1,actual_2)
    t.assert_equals(history_1,history_2)

end


g4.test_1k_rows_rollback_wo_index_wo_batch = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 1} )
    local actual_1 = storage.space.EMPLOYEES_TRANSFER_2:select{}
    local history_1 = storage.space.EMPLOYEES_TRANSFER_HIST_2:select{}

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 2} )

-- luacheck: max line length 150
    local res,err = storage:call('reverse_history_in_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 2, nil} )
    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_2'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST_2'})
    local actual_2 = storage.space.EMPLOYEES_TRANSFER_2:select{}
    local history_2 = storage.space.EMPLOYEES_TRANSFER_HIST_2:select{}


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)
    t.assert_equals(actual_1,actual_2)
    t.assert_equals(history_1,history_2)

end


g4.test_1k_rows_rollback_wo_index_w_batch = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 1} )
    local actual_1 = storage.space.EMPLOYEES_TRANSFER_2:select{}
    local history_1 = storage.space.EMPLOYEES_TRANSFER_HIST_2:select{}

    datagen(1000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 2} )

-- luacheck: max line length 160
    local res,err = storage:call('reverse_history_in_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER_2', 'EMPLOYEES_TRANSFER_HIST_2', 2, 100} )
    local cnt1_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt1_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_2'})
    local cnt1_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST_2'})
    local actual_2 = storage.space.EMPLOYEES_TRANSFER_2:select{}
    local history_2 = storage.space.EMPLOYEES_TRANSFER_HIST_2:select{}


    t.assert_equals(err, nil)
    t.assert_equals(res, true)
    t.assert_equals(cnt1_1,0)
    t.assert_equals(cnt1_2,1000)
    t.assert_equals(cnt1_3,0)
    t.assert_equals(actual_1,actual_2)
    t.assert_equals(history_1,history_2)

end


g4.test_10k_rows_rollback_w_index_w_batch = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(10000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )


    datagen(10000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 2} )

    local actual_2_1 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_2_1 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}


    datagen(10000)

    local _,_ = storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 3} )


-- luacheck: max line length 150
    local res2,err2 = storage:call('reverse_history_in_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 3, 1000} )
    local cnt2_1 = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    local cnt2_2 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER'})
    local cnt2_3 = storage:call('storage_space_count', {'EMPLOYEES_TRANSFER_HIST'})
    local actual_2_2 = storage.space.EMPLOYEES_TRANSFER:select{}
    local history_2_2 = storage.space.EMPLOYEES_TRANSFER_HIST:select{}


    t.assert_equals(err2, nil)
    t.assert_equals(res2, true)
    t.assert_equals(cnt2_1,0)
    t.assert_equals(cnt2_2,10000)
    t.assert_equals(cnt2_3,10000)
    t.assert_equals(actual_2_1,actual_2_2)
    t.assert_equals(history_2_1,history_2_2)
end

g5.test_delete_scd_sql =  function ()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(10000)

    local cnt_before = storage:call('storage_space_count', {'EMPLOYEES_HOT'})

    t.assert_equals(cnt_before,10000)

    local _,err_truncate = storage:call('delete_data_from_scd_table_sql',{'EMPLOYEES_HOT'} )

    t.assert_equals(err_truncate, nil)

    local cnt_after_truncate = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    t.assert_equals(cnt_after_truncate,0)

    datagen(100000)

    local _,err_delete_half = storage:call('delete_data_from_scd_table_sql',{'EMPLOYEES_HOT', '"id" >= 50001'} )
    t.assert_equals(err_delete_half,nil)

    local cnt_after_delete_half = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    t.assert_equals(cnt_after_delete_half,50000)

    local _,err_delete_not_pk = storage:call('delete_data_from_scd_table_sql',{'EMPLOYEES_HOT', [["name" = '123']]})
    t.assert_equals(err_delete_not_pk,nil)

    local cnt_after_delete_not_pk = storage:call('storage_space_count', {'EMPLOYEES_HOT'})
    t.assert_equals(cnt_after_delete_not_pk,0)
end


g6.test_simple_check_data_wo_columns = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end

    datagen(1000)
    local is_gen, res = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen,true)
    t.assert_equals(res,0)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local is_gen2, res2 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen2,true)
    t.assert_equals(res2,1000)
    datagen(1000)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local is_gen3, res3 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1})
    t.assert_equals(is_gen3,true)
    t.assert_equals(res3,1000)

    local is_gen4, res4 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',2})
    t.assert_equals(is_gen4,true)
    t.assert_equals(res4,1000)
end


g6.test_simple_check_data_w_columns = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end
    datagen(1000)
    local is_gen, res = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen,true)
    t.assert_equals(res,0)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local is_gen2, res2 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen2,true)
    t.assert_equals(res2,1181946280889)
    datagen(1000)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local is_gen3, res3 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'}})
    t.assert_equals(is_gen3,true)
    t.assert_equals(res3,1181946280889)

    local is_gen4, res4 = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',2,{'id','sysFrom'}})
    t.assert_equals(is_gen4,true)
    t.assert_equals(res4,1180041276702)
end

g6.test_normalization_checksum_data_w_columns = function()
    local storage = cluster:server('master-1-1').net_box

    local function datagen(number_of_rows)
        for i=1,number_of_rows,1 do
            storage.space.EMPLOYEES_HOT:insert{i,1,'123','123','123',100,0,100}
        end
    end
    datagen(1000)
    local is_gen, res = storage:call('get_scd_table_checksum', {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'},2000000})
    t.assert_equals(is_gen,true)
    t.assert_equals(res,0)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST', 1} )
    local is_gen2, res2 = storage:call(
            'get_scd_table_checksum',
            {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'},2000000}
    )
    t.assert_equals(is_gen2,true)
    t.assert_equals(res2,590474)
    datagen(1000)
    storage:call('transfer_stage_data_to_scd_table',{'EMPLOYEES_HOT', 'EMPLOYEES_TRANSFER', 'EMPLOYEES_TRANSFER_HIST',2} )

    local is_gen3, res3 = storage:call(
            'get_scd_table_checksum',
            {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',1,{'id','sysFrom'},2000000}
    )
    t.assert_equals(is_gen3,true)
    t.assert_equals(res3,590474)

    local is_gen4, res4 = storage:call(
            'get_scd_table_checksum',
            {'EMPLOYEES_TRANSFER','EMPLOYEES_TRANSFER_HIST',2,{'id','sysFrom'},2000000}
    )
    t.assert_equals(is_gen4,true)
    t.assert_equals(res4,589523)
end

g6.test_all_dtm_types_check_data_w_column = function ()
    local storage = cluster:server('master-1-1').net_box
    storage.space.orig__as2__all_types_table_actual:insert{ 1, 7729, 3, nil, 0, 1, 1, 'text', true,
                                                            1, 100000, 18594, tonumber64('1605647472000000ULL'),
                                                            100000000, 'd92beee8-749f-4539-aa15-3d2941dbb0f1', 'c'}

    local is_gen, res = storage:call('get_scd_table_checksum',
        { 'orig__as2__all_types_table_actual',
          'orig__as2__all_types_table_history', 0,
            { "id", "double_col", "float_col", "varchar_col", "boolean_col", "int_col", "bigint_col",
              "date_col", "timestamp_col", "time_col", "uuid_col", "char_col"
            }
        })
    t.assert_equals(is_gen,true)
    t.assert_equals(res,0)
end
