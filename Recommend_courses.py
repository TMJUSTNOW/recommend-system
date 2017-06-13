#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The main operate of recommend system for course use KNNBaselineWithTag.
"""

import numpy as np

from surprise import KNNBaseline
from surprise import Reader

from Recommend_tutors import DatasetUserDatabases
from Recommend_tutors import get_top_n
from Recommend_tutors import save_top_data


class KNNBaselineWithTag(KNNBaseline):
    """
    Rewrite the classical KNNBaseline algorithm to input the tag information
    """

    def get_tag_information(self, connect, table):
        keys = ('tutor_id', 'category_id')
        sep = ', '
        key_all = sep.join(keys)
        sql = 'SELECT COUNT(*) FROM {}'.format(table)
        course_number = connect.select(sql)[0]['COUNT(*)']
#        print(course_number)
        sim_tag = np.zeros((course_number, course_number), np.double)
        sql = 'SELECT id, {}  FROM {}'.format(key_all, table)
        courses = connect.select(sql)
        course_dict = {}
        id_list = []
        for course in courses:
            id = course.pop('id')
            id_list.append(id)
            course_dict[id] = course
#        print(course_dict)
        step = 0.4
        for xi in range(course_number):
            sim_tag[xi, xi] = 1
            for xj in range(xi + 1, course_number):
                for key in keys:
                    if course_dict[id_list[xi]][key] == course_dict[id_list[xj]][key]:
                        sim_tag[xi, xj] = sim_tag[xi, xj] + step
                sim_tag[xj, xi] = sim_tag[xi, xj]
        return sim_tag

    def add_tag_information(self, tagsim):
        print(type(self.sim))


if __name__ == '__main__':
    # Set the useful parameter
    reader = Reader(line_format='user item rating timestamp', sep='\t')
    database = {'host':'localhost', 'id':'tictalk_db', 'codetype':'utf8mb4'}
    user = {'id':'py', 'password':'2151609'}
    table = {'id': 'students_courses', 'item': 'student_id, course_id, trend, created'}

    # Get the u-i rate matrix from database
    data = DatasetUserDatabases(database['host'],database['id'],database['codetype'],reader)
    data.get_data(user['id'], user['password'], table['id'])
    data.build_data(table['item'])

    # build the data for the next operation
    data.split(n_folds=5)
    trainset = data.build_full_trainset()

    # Special design prediction algorithm
    algo = KNNBaselineWithTag()

    algo.train(trainset)

    sim_tag = algo.get_tag_information(data.my_connect, 'courses')
    print(sim_tag)

    algo.add_tag_information(0)

    # Than predict ratings for all pairs (u, i) that are NOT in the training set.
    testset = trainset.build_anti_testset()

    # Get the estimate result
    predictions = algo.test(testset)

    # Get the top n recommend
    top_n = get_top_n(predictions, n=10)

    # Save the result in database
    save_top_data(top_n, data.my_connect, table)
