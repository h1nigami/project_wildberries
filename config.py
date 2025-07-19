import configparser
from typing import Any

class Config():
    def __init__(self, file_name:str):
        self.config = configparser.ConfigParser()
        self.config.read(file_name)
        
    def get_db_url(self) -> Any | None:
        try:
            return str(self.config.get('DATABASE','url'))
        except (configparser.NoOptionError, configparser.NoSectionError):
            return None
    def get_db_name(self):
        try:
            return str(self.config.get('DATABASE', 'db_name'))
        except (configparser.NoOptionError, configparser.NoSectionError):
            raise 'Введите название базы данных'
        
    def get_username(self):
        try:
            return str(self.config.get('DATABASE','user'))
        except(configparser.NoOptionError, configparser.NoSectionError):
            raise 'Введите пользователя базы данных'
        
    def get_password(self):
        try:
             return str(self.config.get('DATABASE','password'))
        except(configparser.NoOptionError, configparser.NoSectionError):
            raise 'Введите пароль к базе данных'
        
config = Config('config.ini')