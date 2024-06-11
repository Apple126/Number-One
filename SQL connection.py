import os

import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

# students_df = pd.read_json(r'C:\Users\User\Desktop\BigData\students.json')
# rooms_df = pd.read_json(r'C:\Users\User\Desktop\BigData\rooms.json')

db_params = {
    'user': 'postgres',
    'password': '12345',
    'host': 'localhost',
    'port': 5432,
    'database': 'Task'
}

engine = create_engine(f'postgresql://{db_params['user']}:{db_params['password']}@{db_params['host']}:{db_params['port']}/{db_params['database']}') # подключение к Postgre SQL

json_directory = r'C:\Users\User\Desktop\BigData'

# определяем json файлы в указанной папке
json_files = []
for i in os.listdir(json_directory):
    if i.endswith('.json'):
        json_files.append(i)
print(f'Выбранные для загрузки в базу файлы: {json_files}')

need_to_upload = input(f'Начать загрузку указанных файлов в базу {db_params['database']}? +/-')
if need_to_upload == '+':
    for file in json_files:
        file_path = os.path.join(json_directory, file)
        table_name = file.split('.')[0]
        df = pd.read_json(file_path)
        df.to_sql(name=f'{table_name}', con=f'{engine}', if_exists='replace', index=False)
        print(f'Загрузка файла {file} в таблицу {table_name}')

    else:
        print('Загрузка отменена')

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

need_query = True

while need_query:
    do_i_need_query = input('Желаете написать SQL-запрос? [+ или -]: ')

    if do_i_need_query == '-':
        need_query = False
        break

    print('Введите SQL-запрос. Введите пустую строку для завершения ввода: ')

    sql_query = ''
    while True:
        line = input()
        if line == '':
            break
        sql_query += line + ' '

    file_format = input('Введите формат файла для сохранения [json / xml / csv]: ').strip().lower()
    file_name = input('Введите наименование файла: ')

    output_path = fr'C:\Users\User\Desktop\BigData\Results\{file_name}.{file_format}'

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