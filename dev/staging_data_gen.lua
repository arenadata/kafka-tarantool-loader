#!/usr/bin/env tarantool

nb = require('net.box')
conn = nb.connect('admin:memstorage-cluster-cookie@localhost:3304')

local function datagen(space_name, number_of_rows)
    local tuples = {}
    for i=1,number_of_rows,1 do
        conn.space[space_name]:replace({i,'a777a750',0,100})
    end
end

datagen('adb__adg_test__env_test_staging', 100000)
conn:close()

-- transfer_stage_data_to_scd_table('adb__adg_test__env_test_staging', 'adb__adg_test__env_test_actual', 'adb__adg_test__env_test_history', 1)