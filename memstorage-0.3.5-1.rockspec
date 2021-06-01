package = 'memstorage'
version = '0.3.5-1'
source  = {
    url = '/dev/null',
}
-- Put any modules your app depends on here
dependencies = {
    'tarantool',
    'lua >= 5.1',
    'luatest',
    'cartridge == 2.3.0',
    'lulpeg == 0.1.2-1',
    'ddl',
    'cron-parser',
    'luacheck',
    'avro-schema',
    'metrics == 0.8.0',
    'moonwalker'


}
build = {
    type = 'none';
}
