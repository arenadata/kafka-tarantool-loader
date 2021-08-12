package = 'memstorage'
<<<<<<< HEAD:memstorage-0.7.8-1.rockspec
version = '0.7.8-1'
=======
version = '0.7.7-1'
>>>>>>> dada1ed295e3f2bc52e2e51abe84cb8311dc0a92:memstorage-0.7.7-1.rockspec
source  = {
    branch = 'master',
    url = 'git+https://github.com/arenadata/kafka-tarantool-loader.git'
}
-- Put any modules your app depends on here
dependencies = {
    'tarantool',
    'lua >= 5.1',
    'luatest',
    'cartridge == 2.6.0',
    'crud == 0.7.0-1',
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
