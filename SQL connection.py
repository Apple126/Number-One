import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy import text

students_df = pd.read_json(r'C:\Users\User\Desktop\BigData\students.json')
rooms_df = pd.read_json(r'C:\Users\User\Desktop\BigData\rooms.json')

print(students_df)
print(rooms_df)

engine = create_engine('postgresql://postgres:12345@localhost:5432/Task') # подключение к PostgreSQL

students_df.to_sql(name='students', con=engine, if_exists='replace', index=False,
    dtype={
        'birthday': sqlalchemy.types.TIMESTAMP(),
        'id': sqlalchemy.types.INTEGER(),
        'name': sqlalchemy.types.VARCHAR(length=55),
        'room': sqlalchemy.types.INTEGER(),
        'sex': sqlalchemy.types.CHAR(length=1),
    })

rooms_df.to_sql(name='rooms', con=engine, if_exists='replace', index=False,
    dtype={
        'id': sqlalchemy.types.INTEGER(),
        'name': sqlalchemy.types.VARCHAR(length=55),
    })

with engine.connect() as connection:
    result = connection.execute(text('SELECT * FROM students LIMIT 5;'))
    for row in result:
        print(row)