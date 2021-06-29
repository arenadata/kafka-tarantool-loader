#!/bin/bash
# Runs vinyl and memtx test

# Switching memtx to vinyl engine
sed -i 's/memtx/vinyl/g' test/integration/data/schema_ddl.yml
.rocks/bin/luatest --coverage -v test/
# Switching to memtx engine back
sed -i 's/vinyl/memtx/g' test/integration/data/schema_ddl.yml
.rocks/bin/luatest --coverage -v test/
