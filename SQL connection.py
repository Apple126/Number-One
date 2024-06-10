import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

students_df = pd.read_json(r'C:\Users\User\Desktop\BigData\students.json')
rooms_df = pd.read_json(r'C:\Users\User\Desktop\BigData\rooms.json')

print(students_df)
print(rooms_df)

engine = create_engine('postgresql://postgres:12345@localhost:5432/Task') # подключение к PostgreSQL

# students_df.to_sql(name='students', con=engine, if_exists='replace', index=False,
#     dtype={
#         'birthday': sqlalchemy.types.TIMESTAMP(),
#         'id': sqlalchemy.types.INTEGER(),
#         'name': sqlalchemy.types.VARCHAR(length=55),
#         'room': sqlalchemy.types.INTEGER(),
#         'sex': sqlalchemy.types.CHAR(length=1),
#     })
#
# rooms_df.to_sql(name='rooms', con=engine, if_exists='replace', index=False,
#     dtype={
#         'id': sqlalchemy.types.INTEGER(),
#         'name': sqlalchemy.types.VARCHAR(length=55),
#     })

# with engine.connect() as connection:
#     result = connection.execute(text('SELECT * FROM students LIMIT 5;'))
#     for row in result:
#         print(row)

need_query = True

while need_query:
    do_i_need = input('Желаете написать SQL-запрос? [+ или -]: ')

    if do_i_need == '-':
        need_query = False
        break


    print('''Введите SQL-запрос. Введите пустую строку для завершения ввода:
    
    SELECT *
    FROM rooms
    LEFT JOIN students on rooms.id = students.room
     ''')

    sql_query = ''
    while True:
        line = input()
        if line == '':
            break
        sql_query += line + ' '

    file_format = input('Введите формат файла для сохранения [json / xml / csv]: ').strip().lower()
    file_name = input('Введите наименование файла: ')

    output_path = f'C:\\Users\\User\\Desktop\\BigData\\Results\\{file_name}.{file_format}'

    # Выполнение SQL-запроса и сохранение результатов в DataFrame
    df_sql_query = pd.read_sql_query(sql_query, engine)

    # Сохранение DataFrame в папку

    if file_format == 'json':
        df_sql_query.to_json(output_path, orient='records')
    elif file_format == 'xml':
        df_sql_query.to_xml(output_path)
    elif file_format == 'csv':
        df_sql_query.to_csv(output_path, index=True)

    print(f'Результат запроса в файле {file_name}.{file_format} сохранен по пути: {output_path}')