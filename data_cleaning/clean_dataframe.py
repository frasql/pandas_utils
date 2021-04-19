import pandas as pd


class CleanDataFrame:
    """
    Object that allows to clean a generic dataframe 
    """
    def __init__(self, df, clean_col=False):
        self.df = df

        if clean_col:
            try:
                self.df.rename(columns=self.clean_columns, inplace=True)
            except:
                raise ValueError
        try:
            self.replace_none(self.df)
        except:
            ValueError
                
    @staticmethod
    def clean_columns(col_to_clean):
        return col_to_clean.strip().lower().replace(' ', '_').replace('à', 'a')

    @property
    def get_dataframe(self):
        return self.df

    def replace_none(self):
        if self.df.isna():
            self.df.fillna(-9999)
            
    def __repr__(self):
        return f'CleanDataframe({self.df.columns})'
