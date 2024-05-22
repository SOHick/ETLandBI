import numpy as np
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pandas as pd

adult = pd.read_csv("data/netflix_titles.csv", encoding='latin1')
adult.drop(adult.columns[12:], axis=1, inplace=True)
print(adult.columns)
