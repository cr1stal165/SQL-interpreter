import csv
from pyparsing import CaselessKeyword, Word, alphas, alphanums, Forward, Group, Optional, delimitedList, ZeroOrMore, \
    oneOf, quotedString, removeQuotes, ParseException, ParseResults, nums
from tabulate import tabulate


def parse_sql(sql):
    # Ключевые слова SQL
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

    def interpret_sql(result):
        selected_fields = result[0][1]  # Получаем список выбранных полей
        table_name = result[0][3][0]  # Получаем имя таблицы
        file_name = table_name + '.csv'

        def select_function(fields, name_table) -> list:
            if '*' in fields[0]:
                fields = reader.fieldnames
            else:
                # Проверка на существование колонок
                fieldnames = reader.fieldnames
                for field in fields:
                    if field not in fieldnames:
                        print(f"Ошибка: Колонка '{field}' не существует в таблице '{name_table}'")
                        return

            table_data = []
            for row in reader:
                table_row = [row[field] for field in fields]
                table_data.append(table_row)
            print(result[0])
            for item in range(len(result[0])):

                if result[0][item] == 'JOIN':
                    print(True)


                if result[0][item] == 'WHERE':
                    logical_operator = 'AND'
                    logical_operator_found = any(
                        isinstance(element, str) and element.upper() == logical_operator for element in result[0][item + 1])
                    print(logical_operator_found)
                    print(result[0][item + 1])
                    if logical_operator_found:
                        curr_list = result[0][item + 1]
                        for i in range(len(curr_list)):
                            if i % 2 == 0:

                                first_order_field = result[0][item + 1][i][0]
                                first_operation = result[0][item + 1][i][1]
                                first_value = result[0][item + 1][i][2]
                                first_sorted_index = None
                                for ind in range(len(fields)):
                                    if first_order_field == fields[ind]:
                                        first_sorted_index = ind

                                print(first_sorted_index)

                                match first_operation:
                                    case '>':
                                        table_data = [row for row in table_data if row[first_sorted_index] > first_value]
                                    case '<':
                                        table_data = [row for row in table_data if row[first_sorted_index] < first_value]
                                    case '=':
                                        table_data = [row for row in table_data if row[first_sorted_index] == first_value]

                    else:
                        order_field = result[0][item + 1][0][0]
                        operation = result[0][item + 1][0][1]
                        value = result[0][item + 1][0][2]
                        sorted_index = None
                        for ind in range(len(fields)):
                            if order_field == fields[ind]:
                                sorted_index = ind

                        print(sorted_index)

                        match operation:
                            case '>':
                                table_data = [row for row in table_data if row[sorted_index] > value]
                            case '<':
                                table_data = [row for row in table_data if row[sorted_index] < value]
                            case '=':
                                table_data = [row for row in table_data if row[sorted_index] == value]

                if result[0][item] == 'ORDER BY':
                    order_by_fields = result[0][-1]
                    print(order_by_fields)
                    if order_by_fields:
                        sort_field = order_by_fields[0]  # Первое поле для сортировки
                        sort_order = order_by_fields[1].upper()  # Порядок сортировки (ASC или DESC)
                        reverse_flag = True if sort_order == 'DESC' else False
                        sorted_index = None
                        for ind in range(len(fields)):
                            if fields[ind] == sort_field:
                                sorted_index = ind

                        table_data = sorted(table_data, key=lambda x: int(x[sorted_index]) if x[sorted_index].isnumeric() else str(x[sorted_index]), reverse=reverse_flag)
                else:
                    pass

            return table_data

        try:
            with open(file_name, newline='') as csvfile:
                reader = csv.DictReader(csvfile)
                curr_table = select_function(selected_fields, table_name)

                print(tabulate(curr_table, headers=selected_fields, tablefmt="pretty"))

        except FileNotFoundError:
            print(f"Файл {file_name} не найден")

    # Функция для вывода AST в виде дерева
    def print_ast(ast, level=0, last_child=False):
        prefix = '|  ' * level
        if level > 0:
            prefix += '└─ ' if last_child else '├─ '

        if isinstance(ast, ParseResults):
            #print(prefix + str(ast)) # убрать это для нормального вывода
            child_count = len(ast)
            for i, item in enumerate(ast):
                print_ast(item, level + 1, last_child=(i == child_count - 1))
        else:
            print(prefix + str(ast))

    try:
        result = sql_stmt.parseString(sql, parseAll=True)
        print("\nAbstract syntax tree\n")
        print_ast(result)
        print("\nSQL Result\n")
        interpret_sql(result)
    except ParseException as e:
        print("Ошибка разбора: %s" % e)


sql_query = '''SELECT id, name FROM table_a'''
parse_sql(sql_query)


