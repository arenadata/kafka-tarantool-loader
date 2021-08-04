#!/usr/bin/env tarantool
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

local lpeg = require("lulpeg")
local locale = lpeg.locale;
-- local yaml = require('yaml')
local fun = require('fun')

local P = lpeg.P;
-- local R = lpeg.R;
local S = lpeg.S;
local V = lpeg.V;
local C = lpeg.C;
-- local Cb = lpeg.Cb;
-- local Cc = lpeg.Cc;
-- local Cf = lpeg.Cf;
-- local Cg = lpeg.Cg;
-- local Cp = lpeg.Cp;
-- local Cs = lpeg.Cs;
-- local Ct = lpeg.Ct;
-- local Cmt = lpeg.Cmt;

-- local white = lpeg.S(" \t\r\n") ^ 0

-- local integer = white * lpeg.R("09") ^ 1 / tonumber
-- local muldiv = white * lpeg.C(lpeg.S("/*"))
-- local addsub = white * lpeg.C(lpeg.S("+-"))


local function caseless (literal)
    local caseless = lpeg.Cf((lpeg.P(1) / function (a) return lpeg.S(a:lower()..a:upper()) end)^1, function (a, b) return a * b end)
    return assert(caseless:match(literal))
end

local K = caseless

local function node(p)
    return p / function(...)
        return {...}
    end
end

local function lower(s)
    return string.lower(s)
end

local function CL(p)
    return p / lower
end

local function binary_op(p)
    return p / function(left, op, right)
        return { op, left, right }
    end
end

local function printf (...)
    print(string.format(...))
end

local function debug (grammar, printer)
    printer = printer or printf
    -- Original code credit: http://lua-users.org/lists/lua-l/2009-10/msg00774.html
    for k, p in pairs(grammar) do
-- luacheck: ignore s ...
        local enter = lpeg.Cmt(lpeg.P(true), function(s, p, ...)
            printer("ENTER %s", k) return p end)
            local leave = lpeg.Cmt(lpeg.P(true), function(s, p, ...)
            printer("LEAVE %s", k) return p end) * (lpeg.P("k") - lpeg.P "k");
        grammar[k] = lpeg.Cmt(enter * p + leave, function(s, p, ...)
            printer("---%s---", k) printer("pos: %d, [%s]", p, s:sub(1, p-1)) return p end)
    end
    return grammar
end


local param_counter = 0

local function param(p)
    return p / function()
        param_counter = param_counter + 1
        return {'param', param_counter}
    end
end


local mysql = locale({
    V("sql_stmt"),
    sql_stmt = V("space")^0 * ( V("select_stmt") ) * V("space")^0 * (P(";") + -1),
    select_stmt = node(
            CL(K("SELECT")) * V "space"^0 * V "select_expr_list" * V "space"^0 * V "from_clause"^-1),
    from_clause =
    K("FROM") * V "space"^0 * V "table_references" * V "space"^0 * V "where_clause"^-1,
    where_clause = K("WHERE") * V "space"^0 * V "where_condition",

    select_expr_list = node(V "select_expr" * V "space"^0 * ( P "," * V "space"^0 * V "select_expr" )^0),
    select_expr = C(V"space"^0 * P("*")) + C(( V"space"^0 * V "table_name" * P "." * P "*" )) + V "column_item",  -- TODO sql function

    table_references = S'"' * C(V "table_name") * S'"' + C(V "table_name"),
    where_condition = V "expr",

    column_item = ( binary_op(V "expr" * V "space"^0 * CL(K("AS")) * V "space"^0 * C(V "column_alias"))
            + V "expr" ),


    expr = V "space"^0 * (
            V "andor" +
                    V "comp" +
                    V "addsub" +
                    V "muldiv" +
                    V "unary" +
                    V "value" ),

    andor = binary_op(
            (V "comp" + V "addsub" + V "muldiv" + V "unary" + V "value") *
                    V "andor_op" *
                    (V "andor" + V "comp" + V "addsub" + V "muldiv" + V "unary" + V "value")
    ),
    comp = binary_op(
            (V "addsub" + V "muldiv" + V "unary" + V "value") *
                    V "comp_op" *
                    (V "comp" + V "addsub" + V "muldiv" + V "unary" + V "value")
    ),
    addsub = binary_op(
            (V "muldiv" + V "unary" + V "value") *
                    V "addsub_op" *
                    (V "addsub" + V "muldiv" + V "unary" + V "value")
    ),
    muldiv = binary_op(
            (V "unary" + V "value") *
                    V "muldiv_op" *
                    (V "muldiv" + V "unary" + V "value")
    ),
    unary = node(
            V "unary_op" *
                    (V "value")
    ),

    unary_op = V "space"^0 * C(K("not") + P "!" + P "-"),
    muldiv_op = V "space"^0 * C(S "*/"),
    addsub_op = V "space"^0 * C(S "+-"),
    andor_op = V "space"^0 * C(K"and" + K"or"),

    comp_op = V "space"^0 * C(
            P "="
                    + P ">="
                    + P ">"
                    + P "<="
                    + P "<"
                    + P "<>"
                    + P "!="),

    value = (
            V "literal_value"
                    + V "column_expr"
                    + V "variable"
                    + V "param"
    ),

    param = param( V "space"^0 * S "?"),

    column_expr = V"space"^0 * C (V "three_part_column_expr" + V "two_part_column_expr" + V "single_part_column_expr" ),

    single_part_column_expr =  V "column_name",
    two_part_column_expr = V "table_name" * P "." * V "column_name",
    three_part_column_expr = V "schema_name" * P "." * V "table_name" * P "." * V "column_name",


    schema_name = V "name",
    column_alias = V "name",
    column_name = V "name",
    table_name = V"space"^0 * V "name",

    variable = V"space"^0 * C(V "name"),

    literal_value = V"space"^0 * (V "numeric_literal" + V "string_literal" + P "NULL" + P "CURRENT_TIME" +
                    P "CURRENT_DATE" + P "CURRENT_TIMESTAMP"),  -- see http://dev.mysql.com/doc/refman/5.5/en/literals.html
    numeric_literal = (V "digit"^1) / tonumber,  -- not enough, see http://dev.mysql.com/doc/refman/5.5/en/number-literals.html
    string_literal = ( P "_" * V "charset_name" + caseless "n" )^-1 * V "real_string_literal",
-- luacheck: max line length 180
    real_string_literal = P '"' * C(( 1 - P '"' )^0) * P '"' + P "'" * C(( 1 - P "'" )^0) * P "'",  -- not enough, see http://dev.mysql.com/doc/refman/5.5/en/string-literals.html
    charset_name = C(V "name"),

    name = P "`"^-1 * ( V "alnum" + P "_" )^1 * P "`"^-1,
})


local grammar = P(C(mysql))
-- luacheck: ignore grammar_debug
local grammar_debug = P(C(debug(mysql)))


local function merge_and(expr)

    if not (type(expr) == 'table' and expr[1] == "and") then
        return expr
    end

    local res = {"and"}

    for _, sub in fun.tail(expr) do
        local merged = merge_and(sub)

        if type(merged) == 'table' and merged[1] == "and" then
            res = fun.totable(fun.chain(res, fun.tail(merged)))
        else
            table.insert(res, merged)
        end
    end

    return res
end

local function parse(str)
    str = string.gsub(str, "/%*.-%*/", "")

    param_counter = 0
-- luacheck: ignore matched
    local matched, tree = grammar:match(str)

    if tree == nil then
        return nil
    end

    -- Simplify/flatten logical and expressions
    if tree[1] == 'select' and tree[4] ~= nil then
        tree[4] = merge_and(tree[4])
    end

    return tree
end



return {
    parse = parse
}
