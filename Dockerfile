# Используем официальный образ Python в качестве базового
FROM python:3.10-slim

# Установим зависимости для работы с PostgreSQL
RUN apt-get update && apt-get install -y libpq-dev

# Установим рабочую директорию внутри контейнера
WORKDIR /app

# Скопируем файлы проекта в рабочую директорию
COPY . .

# Установим зависимости Python из файла requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Определим команду для запуска скрипта
CMD ["python", "Task_1.py"]