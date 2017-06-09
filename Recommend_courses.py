#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""
The main operate of recommend system for course use KNNBaselineWithTag.
"""

import SetData
import random
import time

from surprise import KNNBaseline
from surprise import Dataset

from surprise import Reader
from collections import defaultdict
