import os


class Config:
    def __init__(self):
        self.DB_URL = os.getenv("DB_URL")
        self.UPDATE_FREQUENCY_MIN = int(os.getenv("UPDATE_FREQUENCY_MIN"))


config = Config()
