#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
Set the intermediate data for recommend system.
"""

import pymysql
import numpy
import time


class GetData:
    """
    Get the original data from database.
    """
    def __init__(self,host,database,charset):
        self.host = host
        self.database = database
        self.charset = charset
        self.result = None
        self.connection = None

    def connect(self, user, password):
        self.connection = pymysql.connect(host=self.host,
                                     user = user,
                                     password = password,
                                     db = self.database,
                                     charset = self.charset,
                                     cursorclass = pymysql.cursors.DictCursor)

    def select(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
                self.result = cursor.fetchall()
                return self.result
        except Exception as e:
            print('select err! as ', e)

    def insert(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print('insert err! as ', e)

    def excute(self, sql):
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(sql)
            self.connection.commit()
        except Exception as e:
            print('excute err! as ',e)

    def show_result(self):
        print(self.result)


class Caculate:
    def __init__(self, host, database, code):
        self.host = host
        self.database = database
        self.code = code
        self.item = None
        self.result = None
        self.table = None

    def connect(self, user, password):
        self.my_connect = GetData(self.host, self.database, self.code)
        self.my_connect.connect(user, password)

    def create_temporary_tables(self, table, item):
        table=table+'_tem'
        tep = ' INT, '
        items = tep.join(item) + ' INT'
        self.item = item
#        print(items)
        sql="SHOW TABLES LIKE '{}';".format(table)
        if self.my_connect.select(sql):
            sql = "DROP TABLE {}".format(table)
            self.my_connect.excute(sql)

        sql = "CREATE TABLE {} ({});".format(table, items)
        self.my_connect.excute(sql)

    def insert_trend(self, table):
        table = table + '_tem'
        if table == 'students_tutors_tem':
            self.table = 'students_tutors_tem'
            tables = ('student_call_logs', 'fans', 'appointments')
            keys = (('student_id','tutor_id','duration','cost','rate'), ('student_id','tutor_id'), ('student_id','tutor_id'))
            weights = ((0, 0, 0.01, 0.02, 1, 1), (0, 0, 5), (0, 0, 10))
            for table, key, weight in zip(tables, keys, weights):
                self.insert_table(table, key, weight)
        elif table == 'students_courses_tem':
            self.table = 'students_courses_tem'
            tables = ('student_course_histories', 'course_comments')
            keys = (('student_id', 'course_id'), ('student_id', 'course_id'))
            weights = ((0, 0, 5), (0, 0, 10))
            for table, key, weight in zip(tables, keys, weights):
                self.insert_table(table, key, weight)
        else:
            raise Exception('wrong input')


    def insert_table(self, table, keys, weight):
        tep = ', '
        key = tep.join(keys)
#        print(key)
        sql = "SELECT {} FROM {};".format(key, table)
#        print(sql)
        self.my_connect.select(sql)
        dict_result = self.my_connect.result
        print(dict_result)
        for line in dict_result:
            self.result = [line.get(key) for key in keys]
#            print(self.result)
            self.insert_data(keys, weight)

    def insert_data(self, keys, weight):
        try:
            nonelocal = self.result.index(None)
            self.result[nonelocal] = 0
        except:
            pass
        self.result.append(1)
#        print(self.result)
        result_mat = numpy.mat(self.result)
        weight_mat = numpy.mat(weight)
        trend_mat = numpy.multiply(result_mat, weight_mat)
        trend = trend_mat.sum()
        trend = round(trend)
        if trend < 5:
            return
        sql = "SELECT * FROM {} WHERE {} = {} AND {} = {};".format(self.table, keys[0], self.result[0], keys[1], self.result[1])
        if self.my_connect.select(sql):
            trend = trend+self.my_connect.result[0].get('trend')
            sql = "UPDATE {} SET {} = {}, {} = {} WHERE {} = {} AND {} = {};".format(self.table, self.item[2], trend, self.item[3], time.time(), keys[0], self.result[0], keys[1], self.result[1])
            self.my_connect.insert(sql)
        else:
            sql = "INSERT INTO {} SET {} = {}, {} = {}, {} = {}, {} = {};".format(self.table, keys[0], self.result[0], keys[1], self.result[1], self.item[2], trend, self.item[3], time.time())
            self.my_connect.insert(sql)


    def replace_table(self, table):
        tem_table = table+'_tem'
        old_table = table+'_old'
        sql = "SHOW TABLES LIKE '{}';".format(old_table)
        if self.my_connect.select(sql):
            sql = "DROP TABLE {};".format(old_table)
            self.my_connect.excute(sql)

        sql = "SHOW TABLES LIKE '{}';".format(table)
        if self.my_connect.select(sql):
            sql = "RENAME TABLE {} TO {};".format(table, old_table)
            self.my_connect.excute(sql)

        sql = "RENAME TABLE {} TO {};".format(tem_table, table)
        self.my_connect.excute(sql)
        sql = "DROP TABLE {};".format(old_table)
        self.my_connect.excute(sql)



if __name__ == '__main__':
    Set = Caculate('localhost', 'tictalk_db', 'utf8mb4')
    Set.connect('py', '2151609')
    tables = ('students_tutors', 'students_courses')
    items = (('student_id', 'tutor_id', 'trend', 'created'), ('student_id', 'course_id', 'trend', 'created'))
    for table, item in zip(tables, items):
        Set.create_temporary_tables(table, item)
        Set.insert_trend(table)
        Set.replace_table(table)



