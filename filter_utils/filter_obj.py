import pandas as pd
from typing import List
import time
from collections import deque


def read_dataframe():
    df = pd.read_csv('22230.csv', sep=";", encoding='utf-8')
    df.rename(columns={'ID Artico': 'id_artico', 'Q.ta': 'qta', 'UM': 'um', 'Codice': 'codice'}, inplace=True)
    df['qta'] = df['qta'].str.replace(',', '.').astype(float)
    df['id_artico'] = df['id_artico'].astype(int)
    return df


class SingleFilter: 


    """
    Object that allow to filter on single column by single(multiple) value(s) of a pandas DataFrame.
    :param tuple_filters for single item a tuple("name_column", "value_to_search")
    :param tuple_filters for multiple items is a tuple("name_column", ["values_to_search"])
    """

    tuple_filters = tuple()

    def __init__(self, df: pd.DataFrame, tuple_filters: tuple = None) -> None:
        # type control
        assert isinstance(df, pd.DataFrame)

        self.series = []
        self.index = 0

        if tuple_filters is not None:
            if self.validate_key(df, tuple_filters):
                self.df = df
                self.df.index.name = 'id'
                self.tuple_filters = tuple_filters
                self._filters_opt()
            else:
                raise KeyError("One of the key doesn't exists")
        else:
            self.df = df
            self.df.index.name = 'id'

   
    def __len__(self):
        return len(self.df.index)

   
    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.df.index):
            raise StopIteration
        else:
            row = self.df.loc[[self.index]]
            self.index += 1
            return row
      
    # check if a dict key exists in dataframe columns
    @staticmethod
    def validate_key(df, tuple_filters):
        if tuple_filters is not None:
            df_col_names = df.columns
            if tuple_filters[0] in df_col_names:
                return True
            return False
        else:
            True

    # create filtered dataframe for each filter
    def _filters_opt(self):
        # type of dict value
        if self.tuple_filters is not None:
            col = self.tuple_filters[0]
            val = self.tuple_filters[1]
            if isinstance(self.tuple_filters[1], list):
                for v in val:
                    filt = self.df[col] == v
                    self.series.append(filt)
                
                self.df = self.df[self.df[col].isin(list(val))]

            else:
                filt = self.df[col] == val
                self.df = self.df[filt]
                self.series.append(filt)

    # read series list and apply them to original dataframe
    def create_dataframe(self):
        if len(self.tuple_filters) != 0:
            return self.df
        else:
            return self.df


if __name__ == '__main__':
   
    # ex. flask request.form
    request_dict = {'id_artico': [0, 1482, 1483], 'qta': 6}
    request_tuple = [(k, v) for k, v in request_dict.items()]
    df = read_dataframe()
    list_dfs = []
    for t in request_tuple:
        filter = SingleFilter(df, t)
        filtered_df = filter.create_dataframe()
        list_dfs.append(filtered_df)
        df = filtered_df

    merged = None

    if len(list_dfs) <= 1:
        merged = list_dfs[0]
    else:
        for l in range(0, len(list_dfs)-1):
            merged = pd.merge(list_dfs[l], list_dfs[l+1], on='id', how='inner', suffixes=('', '_y'))
            merged.drop(merged.filter(regex='_y$').columns.tolist(),axis=1, inplace=True)

    print(merged)
    


