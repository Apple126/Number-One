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

    # sql_query = input('''Введите SQL-запрос, например:
    # SELECT rooms.name, AVG(date_part('year', age(birthday))) as age
    # FROM rooms
    # LEFT JOIN students on rooms.id = students.room
    # GROUP BY rooms.name
    # ORDER BY age
    # LIMIT 5:
    #  ''')

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

    file_format = input('Введите формат файла для сохранения [json \ xlm \ csv]: ')

    # Выполнение SQL-запроса и сохранение результатов в DataFrame
    df_sql_query = pd.read_sql_query(sql_query, engine)

    # Сохранение DataFrame в JSON
    df_sql_query.to_json(r'C:\Users\User\Desktop\BigData\Results\rooms.json', orient='records')