# _*_ coding:utf-8 _*_
# company: RuiDa Futures
# author: zizle

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker

from settings import DB_DIR


class DataBaseWorker(object):
    def __init__(self):
        """
        Initialization connection
        """
        db = 'sqlite:///' + DB_DIR
        engine = create_engine(db, echo=False)
        db_session = sessionmaker(bind=engine)
        self.worker = db_session()

    def close(self):
        self.worker.close()

