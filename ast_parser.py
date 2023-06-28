import csv
from pyparsing import CaselessKeyword, Word, alphas, alphanums, Forward, Group, Optional, delimitedList, ZeroOrMore, \
    oneOf, quotedString, removeQuotes, ParseResults, nums


def parse_sql(sql):

    select_stmt = Forward()
    select_token = CaselessKeyword("SELECT")
    from_token = CaselessKeyword("FROM")
    join_token = CaselessKeyword("JOIN")
    on_token = CaselessKeyword("ON")
    where_token = CaselessKeyword("WHERE")
    and_token = CaselessKeyword("AND")
    or_token = CaselessKeyword("OR")
    comma_token = ","
    as_token = CaselessKeyword("AS")
    order_token = oneOf("ASC DESC", caseless=True)
    order_by_token = CaselessKeyword("ORDER BY")
    group_by_token = CaselessKeyword("GROUP BY")

    # Идентификаторы и имена полей
    identifier = Word(alphas, alphanums + "_")
    field_name = delimitedList(identifier, ".", combine=True)
    field_alias = (identifier + as_token + identifier).setParseAction(lambda tokens: tokens[0] + " AS " + tokens[2])
    field = field_name | field_alias

    # Выражения WHERE
    atom = field | Word(nums) | quotedString.setParseAction(removeQuotes) | Group("(" + select_stmt + ")")
    multop = oneOf("* /")
    plusop = oneOf("+ -")
    expr = Forward()
    term = atom + ZeroOrMore(multop + atom)
    expr << term + ZeroOrMore(plusop + term)
    condition = Forward()
    condition << Group((expr + oneOf("= != < > >= <= LIKE", caseless=True) + expr)
                       | (expr + oneOf("IS NOT", caseless=True) + oneOf("NULL", caseless=True))
                       | (expr + CaselessKeyword("IN") + "(" + delimitedList(expr) + ")")
                       | ("(" + condition + ")"))
    where_clause = Group(where_token + condition)

    # Оператор JOIN
    join_type = oneOf("INNER LEFT RIGHT FULL", caseless=True)
    join_expr = Group(Optional(join_type) + join_token + Group(identifier + Optional(as_token + identifier) +
                      Optional(on_token + condition)))
    join_clause = join_expr + ZeroOrMore(join_expr)

    # SQL запрос
    subquery = Forward()
    subquery << Group("(" + select_stmt + ")")

    select_stmt << Group(select_token + (Group(oneOf("*", delimitedList(field))) | Group(delimitedList(field))) +
                         from_token + Group(identifier | subquery) +
                         ZeroOrMore(join_clause) +
                         Optional(where_token + Group(condition + ZeroOrMore((and_token | or_token) + condition))) +
                         Optional(group_by_token + Group(delimitedList(field))) +
                         Optional(order_by_token + delimitedList(Group(field + Optional(order_token)))) +
                         Optional(";"))
    sql_stmt = select_stmt

    return sql_stmt.parseString(sql, parseAll=True)


def print_ast(ast, level=0, last_child=False):
    prefix = '|  ' * level
    if level > 0:
        prefix += '└─ ' if last_child else '├─ '

    if isinstance(ast, ParseResults):
        child_count = len(ast)
        for i, item in enumerate(ast):
            print_ast(item, level + 1, last_child=(i == child_count - 1))
    else:
        print(prefix + str(ast))

