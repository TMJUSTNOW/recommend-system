#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The main operate of recommend system.
"""

import pymysql
from surprise import Dataset
import SetData


class DatasetUserDatabases(Dataset):
    """
    Get the data from mySQL.
    """
    def __init__(self,host,database,charset,reader=None):

        Dataset.__init__(self,reader)
        self.host = host
        self.database = database
        self.charset = charset
        self.n_folds = 5
        self.shuffle = True
        self.result = None
        self.raw_ratings = None

    def get_data(self,user,password):
        my_connect = SetData.GetData(self.host, self.database, self.charset)
        my_connect.connect(user, password)
        my_connect.select("SELECT * FROM u_data LIMIT 5")
        self.result=my_connect.result

    def build_data(self):
        print(self.result)


if __name__ == '__main__':
    data=DatasetUserDatabases('localhost','test','utf8mb4')
    data.get_data('py','2151609')
    data.build_data()

