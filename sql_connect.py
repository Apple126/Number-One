import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import logging

# Настройка логгирования
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler('app.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

# Функция для проверки подключения
def connection_to_database(db_params):
    try:
        engine = create_engine(f'postgresql://{db_params["user"]}:{db_params["password"]}@{db_params["host"]}:{db_params["port"]}/{db_params["database"]}')
        # Попытка установить фактическое соединение
        engine.connect()
        logger.info('Успешное подключение к базе данных')
        return engine
    except Exception as e:
        logger.error(f'Ошибка при подключении к базе данных: {e}')
        raise  # После записи логов возбуждаем исключение снова

# определяем json файлы в указанной папке
def load_json_files(directory):
    logger.info(f'Указанная директория для JSON файлов: {directory}')
    json_files = []
    for i in os.listdir(directory):
        if i.endswith('.json'):
            json_files.append(i)
    logger.info(f'Выбранные для загрузки в базу файлы: {json_files}')
    return json_files

def upload_json_files(files, directory, db_params, engine):
    need_to_upload = input(f'Требуется ли загрузка указанных файлов в базу {db_params["database"]}? [+ или -]: ')
    if need_to_upload == '+':
        for file in files:
            file_path = os.path.join(directory, file)
            table_name = file.split('.')[0]
            df = pd.read_json(file_path)
            df.to_sql(name=table_name, con=engine, if_exists='replace', index=False)
            logger.info(f'Загрузка данных из {file} в таблицу {table_name}')
    else:
        logger.info('Загрузка отменена')

# Написание SQL запроса к базе
def sql_query(engine):
    need_query = True
    while need_query:
        do_i_need_query = input('Желаете написать SQL-запрос? [+ или -]: ')
        if do_i_need_query == '-':
            need_query = False
            break

        logger.info('Начало ввода SQL-запроса')
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

        try:
            # Выполнение SQL-запроса и сохранение результатов в DataFrame
            df_result = pd.read_sql_query(sql_query, engine)

            # Сохранение результата в папку, с выбранным форматом
            if file_format == 'json':
                df_result.to_json(output_path, orient='records')
            elif file_format == 'xml':
                df_result.to_xml(output_path)
            elif file_format == 'csv':
                df_result.to_csv(output_path, index=True)

            logger.info(f'Результат запроса в файле {file_name}.{file_format} сохранен по пути: {output_path}')
        except Exception as e:
            logger.error(f'Ошибка при выполнении SQL-запроса или сохранении файла: {e}')

def main():
    try:
        logger.info('Загрузка переменных окружения')
        load_dotenv()

        db_params = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME'),
        }

        engine = connection_to_database(db_params)
        json_directory = r'/home/user/Desktop/BigData'
        json_files = load_json_files(json_directory)
        upload_json_files(json_files, json_directory, db_params, engine)
        sql_query(engine)
    except Exception as e:
        logger.info(f'Ошибка во время выполнения скрипта: {e}')
    finally:
        logger.info('Скрипт завершен')

if __name__ == "__main__":
    main()