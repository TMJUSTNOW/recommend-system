#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The main operate of recommend system.
"""

import SetData
import random
import time
from surprise import SVD
from surprise import SVDpp
from surprise import KNNBasic
from surprise import KNNBaseline
from surprise import SlopeOne
from surprise import Dataset

from surprise import Reader
from collections import defaultdict


class DatasetUserDatabases(Dataset):
    """
    Get the data from mySQL.
    """
    def __init__(self, host, database, charset, reader=None):

        Dataset.__init__(self, reader)
        self.host = host
        self.database = database
        self.charset = charset
        self.n_folds = 5
        self.shuffle = True
        self.result = None
        self.raw_ratings = None
        self.my_connect = None

    def get_data(self, user, password, table):
        """
        Get the u-i rate matrix from database
        :param user: user id
        :param password: user password
        :param table: the table id
        :return:
        """
        self.my_connect = SetData.GetData(self.host, self.database, self.charset)
        self.my_connect.connect(user, password)
        self.my_connect.select("SELECT * FROM {}".format(table))
        self.result = self.my_connect.result

    def build_data(self, key):

        self.raw_ratings=[self.parse_line(line, key) for line in self.result]

    @staticmethod
    def parse_line(line, key):
        """
        Parse the data and return them in requested format
        :param line: A line of data from the database
        :param key: The useful key for the data
        :return: format data
        """
        keys = key.split(', ')
        ParseLine = (line.get(id) for id in keys)

        return ParseLine

    def build_full_trainset(self):
        """Do not split the dataset into folds and just return a trainset as
        is, built from the whole dataset.

        User can then query for predictions, as shown in the :ref:`User Guide
        <train_on_whole_trainset>`.

        Returns:
            The :class:`Trainset`.
        """

        return self.construct_trainset(self.raw_ratings)

    def raw_folds(self):

        if self.shuffle:
            random.shuffle(self.raw_ratings)
            self.shuffle = False  # set to false for future calls to raw_folds

        def k_folds(seq, n_folds):
            """Inspired from scikit learn KFold method."""

            if n_folds > len(seq) or n_folds < 2:
                raise ValueError('Incorrect value for n_folds.')

            start, stop = 0, 0
            for fold_i in range(n_folds):
                start = stop
                stop += len(seq) // n_folds
                if fold_i < len(seq) % n_folds:
                    stop += 1
                yield seq[:start] + seq[stop:], seq[start:stop]

        return k_folds(self.raw_ratings, self.n_folds)

    def split(self, n_folds=5, shuffle=True):
        """Split the dataset into folds for future cross-validation.

        If you forget to call :meth:`split`, the dataset will be automatically
        shuffled and split for 5-folds cross-validation.

        You can obtain repeatable splits over your all your experiments by
        seeding the RNG: ::

            import random
            random.seed(my_seed)  # call this before you call split!

        Args:
            n_folds(:obj:`int`): The number of folds.
            shuffle(:obj:`bool`): Whether to shuffle ratings before splitting.
                If ``False``, folds will always be the same each time the
                experiment is run. Default is ``True``.
        """

        self.n_folds = n_folds
        self.shuffle = shuffle


def get_top_n(predictions, n=10):
    """
    Return the top-N recommendation for each user from a set of predictions.

    Args:
        predictions(list of Prediction objects): The list of predictions, as
            returned by the test method of an algorithm.
        n(int): The number of recommendation to output for each user. Default
            is 10.

    Returns:
    A dict where keys are user (raw) ids and values are lists of tuples:
        [(raw item id, rating estimation), ...] of size n.
    """

    # First map the predictions to each user.
    top_n = defaultdict(list)
    for uid, iid, true_r, est, _ in predictions:
        top_n[uid].append((iid, est))

    # Then sort the predictions for each user and retrieve the k highest ones.
    for uid, user_ratings in top_n.items():
        user_ratings.sort(key=lambda x: x[1], reverse=True)
        top_n[uid] = user_ratings[:n]

    return top_n

def save_top_data(top_n, connect, table):

    """
    save the recommend result in mySQL database.
    :param top_n: The top n recommend result for every user.
    :param connect: The connect to database.
    :param table: The table which get the recommend from.
    :return: None
    """

    table_id = table['id'] + '_recommend_result'
    items = table['item'].split(', ')
    create_item = items[0] + ' INT, ' + items[1] + ' VARCHAR(100), ' + items[3]+' INT'

    sql = "SHOW TABLES LIKE '{}';".format(table_id)
    if connect.select(sql):
        sql = "DROP TABLE {}".format(table_id)
        connect.excute(sql)

    sql = "CREATE TABLE {} ({});".format(table_id, create_item)
    connect.excute(sql)
    for uid, user_ratings in top_n.items():
        result = str([iid for (iid, _) in user_ratings])
        sql = "INSERT INTO {} SET {} = {}, {} = \'{}\', {} = {};".format(table_id, items[0], uid, items[1], result, items[3], time.time())
        connect.insert(sql)


if __name__ == '__main__':
    # Set the useful parameter
    reader = Reader(line_format='user item rating timestamp', sep='\t')
    database = {'host':'localhost', 'id':'tictalk_db', 'codetype':'utf8mb4'}
    user = {'id':'py', 'password':'2151609'}
    table = {'id':'students_tutors', 'item':'student_id, tutor_id, trend, created'}

    # Get the u-i rate matrix from database
    data = DatasetUserDatabases(database['host'],database['id'],database['codetype'],reader)
    data.get_data(user['id'], user['password'], table['id'])
    data.build_data(table['item'])

    # build the data for the next operation
    data.split(n_folds=5)
    trainset = data.build_full_trainset()

    # Some different algorithms to select
    algo = SVDpp()
#    algo = SVD()
#    algo = KNNBasic()
#    algo = SlopeOne()

    algo.train(trainset)

    # Than predict ratings for all pairs (u, i) that are NOT in the training set.
    testset = trainset.build_anti_testset()

    # Get the estimate result
    predictions = algo.test(testset)

    # Get the top n recommend
    top_n = get_top_n(predictions, n=10)

    # Save the result in database
    save_top_data(top_n, data.my_connect, table)


