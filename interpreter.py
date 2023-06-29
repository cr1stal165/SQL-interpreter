import csv

import pandas as pd
import pyparsing
from tabulate import tabulate
from ast_parser import parse_sql


def interpret_sql(result):
    print(result)
    selected_fields = result[0][1]
    table_name = result[0][3][0]
    file_name = table_name + '.csv'


    def select_function(fields, name_table) -> list:
        flag = False
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

        for item in range(len(result[0])):

            if len(result[0][item]) > 1 and result[0][item][1] == 'JOIN' or result[0][item][0] == 'JOIN':
                if result[0][item][0] == 'JOIN':
                    result[0][item].insert(0, "INNER")

                type_join = result[0][item][0]
                table_target = result[0][item][2][0]

                condition = result[0][item][2][2]
                key = condition[0].split(".")
                key2 = condition[2].split(".")


                df_a = pd.read_csv(f'resources/{table_name}.csv')
                df_b = pd.read_csv(f'resources/{table_target}.csv')
                merged_df = pd.merge(df_a, df_b, left_on=key[len(key) - 1], right_on=key2[len(key2) - 1], how=type_join.lower())

                filtered_df = merged_df

                if len(result[0]) >= 7:
                    condition_list = result[0][item + 2]

                    logical_operator = None

                    for i in range(len(condition_list)):

                        if isinstance(condition_list[i], pyparsing.results.ParseResults):
                            res = condition_list[i][0].split('.')
                            if res[1] not in df_a.columns or res[1] not in df_b.columns:
                                column = res[1]
                            else:
                                column = f'{res[1]}_x' if res[0] == table_name else f'{res[1]}_y'

                            operator = condition_list[i][1]
                            value = condition_list[i][2]

                            if logical_operator == 'AND':
                                if value.isdigit():
                                    filtered_df = filtered_df[
                                        filtered_df[column] & (filtered_df[column].astype(int) > int(value))]
                                else:
                                    filtered_df = filtered_df[filtered_df[column] & (filtered_df[column] > value)]
                            elif logical_operator == 'OR':
                                if value.isdigit():
                                    filtered_df = filtered_df[
                                        filtered_df[column] | (filtered_df[column].astype(int) > int(value))]
                                else:
                                    filtered_df = filtered_df[filtered_df[column] | (filtered_df[column] > value)]
                            else:
                                if operator == '>':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] > int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] > value]
                                elif operator == '>=':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] >= int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] >= value]
                                elif operator == '<':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] < int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] < value]
                                elif operator == '<=':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] <= int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] <= value]
                                elif operator == '=':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] == int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] == value]
                                elif operator == '!=':
                                    if value.isdigit():
                                        filtered_df = filtered_df[filtered_df[column] != int(value)]
                                    else:
                                        filtered_df = filtered_df[filtered_df[column] != value]

                        elif condition_list[i] == 'AND':
                            condition_flag = 'AND'
                        elif condition_list[i] == 'OR':
                            condition_flag = 'OR'


                merged_df = filtered_df

                if len(result[0]) == 9:
                    order_by_col = result[0][item + 4][0].split('.')
                    sort_ascending = True if result[0][item + 4][1] == 'ASC' else False
                    if order_by_col[1] not in df_a.columns or order_by_col[1] not in df_b.columns:
                        sort_col = order_by_col[1]
                    else:
                        sort_col = f'{order_by_col[1]}_y' if order_by_col[0] == key2[0] else f'{order_by_col[1]}_x'

                    merged_df = merged_df.sort_values(by=sort_col, ascending=sort_ascending)

                table_data = merged_df.values.tolist()
                flag = True


            if result[0][item] == 'WHERE' and flag == False:
                logical_operator = 'AND'
                logical_operator_found = any(
                    isinstance(element, str) and element.upper() == logical_operator for element in result[0][item + 1])
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

                            match first_operation:
                                case '>':
                                    table_data = [row for row in table_data if row[first_sorted_index] > first_value]
                                case '<':
                                    table_data = [row for row in table_data if row[first_sorted_index] < first_value]
                                case '=':
                                    table_data = [row for row in table_data if row[first_sorted_index] == first_value]
                                case '<=':
                                    table_data = [row for row in table_data if row[first_sorted_index] <= first_value]
                                case '>=':
                                    table_data = [row for row in table_data if row[first_sorted_index] >= first_value]

                else:
                    order_field = result[0][item + 1][0][0]
                    operation = result[0][item + 1][0][1]
                    value = result[0][item + 1][0][2]
                    sorted_index = None
                    for ind in range(len(fields)):
                        if order_field == fields[ind]:
                            sorted_index = ind

                    match operation:
                        case '>':
                            table_data = [row for row in table_data if row[sorted_index] > value]
                        case '<':
                            table_data = [row for row in table_data if row[sorted_index] < value]
                        case '=':
                            table_data = [row for row in table_data if row[sorted_index] == value]
                        case '<=':
                            table_data = [row for row in table_data if row[sorted_index] <= value]
                        case '>=':
                            table_data = [row for row in table_data if row[sorted_index] >= value]

            if result[0][item] == 'ORDER BY' and flag == False:
                order_by_fields = result[0][-1]
                if order_by_fields:
                    sort_field = order_by_fields[0]  # Первое поле для сортировки
                    sort_order = order_by_fields[1].upper()  # Порядок сортировки (ASC или DESC)
                    reverse_flag = True if sort_order == 'DESC' else False
                    sorted_index = None
                    for ind in range(len(fields)):
                        if fields[ind] == sort_field:
                            sorted_index = ind

                    table_data = sorted(table_data,
                                        key=lambda x: int(x[sorted_index]) if x[sorted_index].isnumeric() else str(
                                            x[sorted_index]), reverse=reverse_flag)
            else:
                pass

        return table_data

    # join, подзапрос
    try:
        with open(f'resources/{file_name}', newline='') as csvfile:
            reader = csv.DictReader(csvfile)
            curr_table = select_function(selected_fields, table_name)

            print(tabulate(curr_table, headers=selected_fields, tablefmt="pretty"))

    except FileNotFoundError:
        print(f"Файл {file_name} не найден")


sql_query = '''SELECT * FROM table_a INNER JOIN table_b ON table_a.age = table_b.id'''

result = parse_sql(sql_query)
interpret_sql(result)
