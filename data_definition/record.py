from .manipulate import decode_header, rename_header, decode_record, ManipulateDataFrame
import time
import pandas as pd
from pprint import pprint
import numpy as np


filename = 'ricambi.csv'

start = time.time()

# load csv file
with open(filename, 'rb') as f:
    # read header from generator
    header = next(f)
    # dict rename header
    values_to_rename = {
        'TIPOLOGIA':'Tipologia', 
        'VIA': 'Via',
        'CAP': 'Cap',
        'CITTA':'Citta',
        'STATO': 'Stato',
        'IC figlio': 'IC',
        'Qta Figlio': 'Qta',
        'VALORE': 'Valore',
        'IDArticoComponente': 'idartico'
    }

    # decode binary header, replace and strip
    header_list = decode_header(header)
    print("Header Decoded")

    # rename column header
    header_renamed = rename_header(header_list, **values_to_rename)
    print("Header Renamed")

    # loop through records generator
    records = []
    for record in f:
        # decode binary record, replace and strip
        rec = decode_record(record)
        # appned cleaned report
        records.append(rec)


    print(len(header_renamed))
    # create index for empty dataframe
    index = np.arange(start=0, stop=len(records))
    # create new dataframe with header and index
    dataframe = pd.DataFrame(columns=header_renamed, index=index)
    print("********************************************************************")
    print(f"Dataframe shape before inserting & deleting rows {dataframe.shape}")
    print("********************************************************************")

    # loop through dataframe length
    for i in range(0, len(index)):
            # insert record in empty dataframe
            dataframe.iloc[i] = records[i]

    # list of column to drop
    col_to_drop = [
        "idLO",
        "Anno OP",
        "Numero OP",
        "Data OP",
        "TipoRiga",
        "idartico",
        "IDVARDB"
    ]

    # list of columns to insert
    col_to_insert = [
        (0, "Valore Unitario", 0),
    ]

    # insert columns to dataframe
    man_df = ManipulateDataFrame(dataframe, col_to_insert=col_to_insert, col_to_drop=col_to_drop)

    """
    print(len(header_renamed), header_renamed, sep=",")
    print(len(records[0]), records[0], sep=",")
    """  
    print("********************************************************************")
    print(f"Dataframe shape after inserting & deleting rows {dataframe.shape}")
    print("********************************************************************")
    
    end = (time.time() - start) 

    print("**********************************************************************")
    print(f'Time to read and process rows -> Minute: {end / 60}, Seconds: {end}')
    print("**********************************************************************")
    
    dataframe.to_csv('new_dataframe.csv', encoding='utf-8', sep=';')


    
