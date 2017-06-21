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
        self.sim_tag = np.zeros((course_number, course_number), np.double)
        sql = 'SELECT id, {}  FROM {}'.format(key_all, table)
        courses = connect.select(sql)
        course_dict = {}
        id_list = []
        for course in courses:
            id = course.pop('id')
            id_list.append(id)
            course_dict[id] = course
#        print(course_dict)
        step = {'tutor_id':0.5, 'category_id':0.4}
        for xi in range(course_number):
            self.sim_tag[xi, xi] = 1
            for xj in range(xi + 1, course_number):
                for key in keys:
                    if course_dict[id_list[xi]][key] == course_dict[id_list[xj]][key]:
                        self.sim_tag[xi, xj] = self.sim_tag[xi, xj] + step[key]
                self.sim_tag[xj, xi] = self.sim_tag[xi, xj]

    def add_tag_information(self):
        shape_ui = self.sim.shape
        shape_is = self.sim_tag.shape
        sim_fix = np.zeros((shape_is[0], shape_is[1]), np.double)
        if shape_ui == shape_is:
            for xi in range(shape_is[0]):
                sim_fix[xi, xi] = 1
                for xj in range(xi + 1, shape_is[0]):
                    sim_fix[xi, xj] = min(self.sim[xi, xj] + self.sim_tag[xi, xj], 1)
                    sim_fix[xj, xi] = sim_fix[xi, xj]
        else:
            print('Error! The shape of similarity matrix is not same!')
            sim_fix = self.sim_tag
#        print(self.sim)
        self.sim = sim_fix
#        print(self.sim)
#        self.sim = np.zeros((shape_is[0], shape_is[1]), np.double)
#        print(self.sim_tag)
#        print(self.sim)
#        print(sim_fix)


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
    sim_options = {'name': 'pearson_baseline', 'user_based': False}
#    sim_options = {'name': 'cosine', 'user_based': False}
    algo = KNNBaselineWithTag(sim_options=sim_options)

    algo.train(trainset)
#    print(algo.sim)

    algo.get_tag_information(data.my_connect, 'courses')

    algo.add_tag_information()
#    print(algo.sim)

    # Than predict ratings for all pairs (u, i) that are NOT in the training set.
    testset = trainset.build_anti_testset()
#    print(testset)

    # Get the estimate result
    predictions = algo.test(testset)

    # Get the top n recommend
    top_n = get_top_n(predictions, n=10)

    # Save the result in database
    save_top_data(top_n, data.my_connect, table)

    data.my_connect.close()
