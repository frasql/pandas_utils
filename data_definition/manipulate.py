from typing import List
import numpy as np

# decode and split header from bin to list
def decode_header(values):
    try:
        header_decoded = values.decode('utf-8').replace('\n', '').split(';')
    except:
        raise ValueError("Error! Unable to decode the header")
    return header_decoded


# rename header with dict(old_name=new_name)
def rename_header(header, **kwargs):
    if not isinstance(header, list):
        raise ValueError('Only list objects are allowed.')

    try:
        for i, k in enumerate(header):
            if k in kwargs.keys():
                header[i] = kwargs[k]
    except:
        raise ValueError('Error! Unable to rename values.')
    return header


# decode and split record from bin to list
def decode_record(values):
    record_decoded = values.decode('utf-8').replace('\n', '').strip().split(';')
    record_strip = [x.strip() for x in record_decoded]
    
    for i, r in enumerate(record_strip):
        if r == '':
            record_strip[i] = np.nan

    return record_strip



class TupleInsert:
    position = 0
    col_name = None
    default_value = 0
    def __init__(self, position=None, col_name=None, default_value=None):
        # validate position
        if position == None:
            pass
        # initialize position
        self.position = position
        # validate col_name
        if col_name is None:
            raise ValueError("Devi inserire il nome della colonna da aggiungere")
        # initialize col_name
        self.col_name = col_name

        if default_value != None:
            self.default_value = default_value

    def __repr__(self):
        return f"<TupleInsert(position={self.position}, col_name='{self.col_name}', default_value={self.default_value})>"



class ManipulateDataFrame:
    df = None
    col_to_drop = []
    col_to_insert = []
    def __init__(self, df, col_to_insert=None, col_to_drop=None):
        # validate df
        if df is None:
            raise ValueError("Inserisci il dataframe da manipolare")
        # initialize df
        self.df = df

        # validate col_to_insert
        if col_to_insert != None:
            if not isinstance(col_to_insert, list):
                raise TypeError("Le colonne/a che inserisci devono essere un tipo List")
            self.col_to_insert = col_to_insert

        # validate col_to_drop
        if col_to_drop != None:
            if not isinstance(col_to_drop, list):
                raise TypeError("Le colonne/a da eliminare devono essere un tipo List")
            self.col_to_drop = col_to_drop

    def insert_columns_into_df(self):
        for i, _ in self.col_to_insert:
            col_insert = TupleInsert(position=self.col_to_insert[i][0], col_name=self.col_to_insert[i][1], default_value=self.col_to_insert[i][2])
            self.df.insert(col_insert.position, column=col_insert.col_name, default=col_insert.default_value)
        return self

    def drop_columns_from_df(self):
        self.df.drop(self.col_to_drop)
        return self


    def __repr__(self):
        return f"<ManipulateDataFrame(df={self.df.index.name}, col_to_insert={self.col_to_insert}, col_to_drop={self.col_to_drop})>"
        



        


