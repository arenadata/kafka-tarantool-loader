local lcstd = require('luacheck.standards')

local empty = {}

local table_defs = lcstd.def_fields(
    "maxn", "copy", "new", "clear", "move", "foreach", "sort", "remove",
    "foreachi", "deepcopy", "getn", "concat", "insert", "contains"
)

local io_file_defs = lcstd.def_fields(
    "__gc", "__index", "__tostring", "write", "read", "close", "flush", "seek",
    "lines", "setvbuf"
)

local string_defs = lcstd.def_fields(
    "byte", "char", "dump", "find", "format", "gmatch", "gsub", "len", "lower",
    "match", "rep", "reverse", "sub", "upper", "startswith", "endswith",
    "split", "ljust", "rjust", "center", "startswith", "endswith", "hex",
    "strip", "lstrip", "rstrip"
)

local io_defs = {}
io_defs.fields = {
    close   = empty,
    flush   = empty,
    input   = empty,
    lines   = empty,
    open    = empty,
    output  = empty,
    popen   = empty,
    read    = empty,
    stderr  = io_file_defs,
    stdin   = io_file_defs,
    stdout  = io_file_defs,
    tmpfile = empty,
    type    = empty,
    write   = empty,
}

local jit_defs = lcstd.def_fields(
    "arch", "version", "version_num", "status", "on", "os", "off", "flush",
    "attach", "opt"
)

local math_defs = lcstd.def_fields(
    "ceil", "tan", "log10", "randomseed", "cos", "sinh", "random", "huge",
    "pi", "max", "atan2", "ldexp", "floor", "sqrt", "deg", "atan", "fmod",
    "acos", "pow", "abs", "min", "sin", "frexp", "log", "tanh", "exp",
    "modf", "cosh", "asin", "rad"
)

local os_defs = lcstd.def_fields(
    "execute", "rename", "environ", "setenv", "setlocale", "getenv",
    "difftime", "remove", "date", "exit", "time", "clock", "tmpname"
)

local package_defs = {}
package_defs.fields = {
    config     = string_defs,
    cpath      = { fields = string_defs.fields, read_only = false },
    loaded     = { other_fields = true,         read_only = false },
    loadlib    = empty,
    path       = { fields = string_defs.fields, read_only = false },
    preload    = { other_fields = true,         read_only = false },
    loaders    = { other_fields = true,         read_only = false },
    searchpath = empty,
    seeall     = empty,
    search     = empty,
    "searchroot"
}

local bit_defs = lcstd.def_fields(
    "rol", "rshift", "ror", "bswap", "bxor", "bor", "arshift", "bnot", "tobit",
    "lshift", "tohex", "band"
)

local globals = {
    -- module
    io        = io_defs,
    jit       = jit_defs,
    math      = math_defs,
    os        = os_defs,
    package   = package_defs,
    string    = string_defs,
    table     = table_defs,
    bit       = bit_defs,

    dostring     = empty,
    tutorial     = empty,
    tonumber64   = empty,

    -- variables
    _G       = { other_fields = true, read_only = false },
    _VERSION = string_defs,
    arg      = { other_fields = true },
    ddl_callbacks = { other_fields = true },

    -- coroutine
    "wrap", "yield", "resume", "status", "isyieldable", "running", "create",

    -- debug
    "traceback", "setlocal", "getupvalue", "setupvalue", "upvalueid",
    "getlocal", "getregistry", "getinfo", "sethook", "setmetatable",
    "upvaluejoin", "gethook", "debug", "getmetatable", "setfenv",
    "getfenv",

    -- functions
    "assert", "collectgarbage", "dofile", "error", "getfenv", "getmetatable",
    "ipairs", "help", "load", "loadfile", "loadstring", "module",
    "next", "pairs", "pcall", "print", "rawget", "rawset",
    "require", "select", "setfenv", "setmetatable", "tonumber", "tostring",
    "type", "unpack", "xpcall", "gcinfo", "newproxy", "get_ddl",

    -- tarantool
    "box", "schema", "tuple", "error", "sequence", "commit", "once", "space",
    "backup", "info", "session", "cfg", "snapshot", "slab", "atomic",
    "rollback_to_savepoint", "savepoint", "begin", "rollback",
    "runtime", "index", "stat", "table_mutex_map",

    -- string defs
    "byte", "char", "dump", "find", "format", "gmatch", "gsub", "len", "lower",
    "match", "rep", "reverse", "sub", "upper", "startswith", "endswith",
    "split", "ljust", "rjust", "center", "startswith", "endswith", "hex",
    "strip", "lstrip", "rstrip",

    -- request lua
    "reload_force", "reload_config", "reload_check", "reload_rollback", "reload_prepare",
    "reload_switch", "reload_hash", "cfg_hash", "delete_state", "schema_change_on_single_server",
    "shard_status_check", "shard_get_uuid", "reconfigure_shard", "status", "start_graphiql",
    "stop_graphiql",
}

write_globals = {
    "ddl_callbacks",
    "reload_force", "reload_config", "reload_check", "reload_rollback", "reload_prepare",
    "reload_switch", "reload_hash", "cfg_hash", "delete_state", "schema_change_on_single_server",
    "shard_status_check", "shard_get_uuid", "reconfigure_shard", "status", "start_graphiql",
    "stop_graphiql",

    "monitor", "wait_lsn", "count", "multi_delete", "multi_update", "write", "check",
    "stop_replication", "set_master", "set_replica",

    -- app lua
    "ready_to_shutdown", "insert", "delete", "read", "update", "monitor",
    "shutdown", "config", "reload", "check",

    -- vshard
    "vshard",
    table = table_defs
}

std = {
    read_globals = globals,
    globals = write_globals
}

ignore = {
    "512" -- loop is executed at most once
}

redefined = false

max_line_length = 140

-- vim: syntax=lua
