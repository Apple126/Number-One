import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv

load_dotenv()

db_params = {
    'user': os.getenv('DB_USER'),
    'password': os.getenv('DB_PASSWORD'),
    'host': os.getenv('DB_HOST'),
    'port': os.getenv('DB_PORT'),
    'database': os.getenv('DB_NAME'),
}

engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}') # подключение к Postgre SQL

json_directory = r'/home/user/Desktop/BigData'

# определяем json файлы в указанной папке
json_files = []
for i in os.listdir(json_directory):
    if i.endswith('.json'):
        json_files.append(i)
print(f'Выбранные для загрузки в базу файлы: {json_files}')

need_to_upload = input(f'Требуется ли загрузка указанных файлов в базу {db_params["database"]}? [+ или -]: ')
if need_to_upload == '+':
    for file in json_files:
        file_path = os.path.join(json_directory, file)
        table_name = file.split('.')[0]
        df = pd.read_json(file_path)
        df.to_sql(name=f'{table_name}', con=engine, if_exists='replace', index=False)
        print(f'Загрузка данных из {file} в таблицу {table_name}')
else:
    print('Загрузка отменена')

# Написание SQL запроса к базе
need_query = True
while need_query:
    do_i_need_query = input('Желаете написать SQL-запрос? [+ или -]: ')
    if do_i_need_query == '-':
        need_query = False
        break

    print('Введите SQL-запрос. Для завершения ввода - введите пустую строку : ')
    # Ввод самого запроса (с возможностью писать в удобном формате, как в SQL, на новых строчках)
    sql_query = ''
    while True:
        line = input()
        if line == '':
            break
        sql_query += line + ' '

    file_format = input('Введите формат файла для сохранения [json / xml / csv]: ').strip().lower()
    file_name = input('Введите наименование файла: ')

    output_path = fr'/home/user/Desktop/BigData/Results/{file_name}.{file_format}'

    # Выполнение SQL-запроса и сохранение результатов в DataFrame
    df_result = pd.read_sql_query(sql_query, engine)

    # Сохранение результата в папку, с выбранным форматом
    if file_format == 'json':
        df_result.to_json(output_path, orient='records')
    elif file_format == 'xml':
        df_result.to_xml(output_path)
    elif file_format == 'csv':
        df_result.to_csv(output_path, index=True)

    print(f'Результат запроса в файле {file_name}.{file_format} сохранен по пути: {output_path}')