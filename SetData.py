#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Set the intermediate data for recommend system.
"""

import pymysql
import json

class GetData:
    """
    Get the original data from database.
    """
    def __init__(self,host,database,charset):
        self.host=host
        self.database=database
        self.charset=charset
        self.result=0;

    def connect(self,user,password):
        self.connection = pymysql.connect(host=self.host,
                                     user=user,
                                     password=password,
                                     db=self.database,
                                     charset=self.charset,
                                     cursorclass=pymysql.cursors.DictCursor)

    def select(self,sql):
        with self.connection.cursor() as cursor:
            cursor.execute(sql)
            self.result = cursor.fetchall()

    def show_result(self):
        print(self.result)



if __name__ == '__main__':
    my_connect=GetData('localhost','tictalk_db','utf8mb4')
    my_connect.connect('py','2151609')
    my_connect.select("SELECT `id`, `nickname` , `mobile` FROM `students` LIMIT 5")
    my_connect.show_result()
