from surprise import SVD
from surprise import Dataset
from surprise import evaluate, print_perf
import os
from surprise import Reader


# path to dataset file
file_path = os.path.expanduser('~/文档/data/ml-100k/u.data')

# As we're loading a custom dataset, we need to define a reader. In the
# movielens-100k dataset, each line has the following format:
# 'user item rating timestamp', separated by '\t' characters.
reader = Reader(line_format='user item rating timestamp', sep='\t')

data = Dataset.load_from_file(file_path, reader=reader)
data.split(n_folds=5)

# We'll use the famous SVD algorithm.
algo = SVD()

# Evaluate performances of our algorithm on the dataset.
perf = evaluate(algo, data, measures=['RMSE', 'MAE'])

print_perf(perf)
