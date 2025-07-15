import psycopg2
from config import config

DATABASE_URL = config.get_db_url()

db_name = config.get_db_name()
db_user = config.get_username()
db_password = config.get_password()


class DB():
    def __init__(self, name, user, password):
        self.connect = psycopg2.connect(f'dbname={name} user={user} password={password}')
        self.cur = self.connect.cursor()

    def create_table(self, name:str):
        query = f'''CREATE TABLE {name} (id bigint primary key,
            name varchar,
            price integer,
            salePriceU integer,
            cashback integer,
            sale integer,
            brand varchar,
            rating double,
            supplier varchar,
            supplierRating double,
            feedbacks varchar,
            reviewRating double,
            promoTextCard varchar,
            promoTextCat varchar,
            link varchar,
        )'''
        self.cur.execute(query=query)
        return self.connect.commit()
        
db = DB(name=db_name, user=db_user, password=db_password)

