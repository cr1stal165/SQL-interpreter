from ast_parser import print_ast, parse_sql

sql_query = '''
  SELECT id, name, city, age
    FROM (
      SELECT *
        FROM table_b
        WHERE table_a.city > (
                SELECT *
                  FROM table_b
                       JOIN table_a ON 
                         col3 = col5
              ) 
        ORDER BY col_a DESC
    )
'''
result = parse_sql(sql_query)
print(result)
print_ast(result)
