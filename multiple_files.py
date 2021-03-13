from concurrent.futures import ThreadPoolExecutor
import pandas as pd
import os


# create file generator
def read_file(file):
    df = pd.read_pickle(file)
    yield df


# list all files
all_files = [f for f in os.listdir('.') if f.endswith('.pkl')]


# read and concat with thread pool
with ThreadPoolExecutor() as exeutor:
    result = exeutor.map(read_file, all_files)
    results = [next(dataframe) for dataframe in result]
    concat = pd.concat(results, axis=0, ignore_index=True)
    concat.to_pickle('dataframe_concatenated.pkl')


