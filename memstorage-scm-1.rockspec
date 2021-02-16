package = 'memstorage'
version = 'scm-1'
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
    'metrics == 0.1.8',
    'moonwalker'


}
build = {
    type = 'none';
}
