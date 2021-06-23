#!/bin/bash
# Runs only vinyl test

# Switch memtx to vinyl engine if needed
sed -i 's/vinyl/memtx/g' test/integration/data/schema_ddl.yml
.rocks/bin/luatest --coverage -v test/
