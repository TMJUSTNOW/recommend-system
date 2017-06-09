#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The main operate of recommend system for course use KNNBaselineWithTag.
"""

from surprise import KNNBaseline
from surprise import Reader

from Recommend_tutors import DatasetUserDatabases
from Recommend_tutors import get_top_n
from Recommend_tutors import save_top_data


class KNNBaselineWithTag(KNNBaseline):
    """
    Rewrite the classical KNNBaseline algorithm to input the tag information
    """

    def GetTagInformation(self, connect, table):
        pass

    def AddTagInformation(self, tagsim):
        pass


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

    # Than predict ratings for all pairs (u, i) that are NOT in the training set.
    testset = trainset.build_anti_testset()

    # Get the estimate result
    predictions = algo.test(testset)

    # Get the top n recommend
    top_n = get_top_n(predictions, n=10)

    # Save the result in database
    save_top_data(top_n, data.my_connect, table)
