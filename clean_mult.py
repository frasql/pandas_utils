import mysql.connector
import pandas as pd  
import numpy as np 
from pprint import pprint
import pickle
import time
import concurrent.futures


class ChainedAssignent:

    """
        Context manager to temporarily set pandas chained assignment warning. Usage:
    
        with ChainedAssignment():
             blah  
             
        with ChainedAssignment('error'):
             run my code and figure out which line causes the error! 
    
    """


    def __init__(self, chained = None):
        acceptable = [ None, 'warn','raise']
        assert chained in acceptable, "chained must be in " + str(acceptable)
        self.swcw = chained

    def __enter__( self ):
        self.saved_swcw = pd.options.mode.chained_assignment
        pd.options.mode.chained_assignment = self.swcw
        return self

    def __exit__(self, *args):
        pd.options.mode.chained_assignment = self.saved_swcw



def read_dataframe():
    df = pd.read_pickle('#')
    col_names = ['codice_primario', 'descrizione_primario', 'id_primario', 'codcie_secondario', 'descrizione_secondario', 'id_secondario', 'qta_totale', 'id_artico']
    df.columns = col_names
    return df


def group_dataframe(df):
        df_group = df.groupby('id_artico')
        for name, group in df_group:
            group = df_group.get_group(name)
            yield group
        # codice_espolo = main_mult_mrp(group)
        # distinta_tables.append(codice_espolo)


def group_artico(df):
    artico_grouped = {}
    for name, group in  df.groupby('id_artico'):
        artico_grouped.update({name: group})

    return artico_grouped


def extract_qta(group):
    qta_totale = {}
    group.reset_index(inplace=True, drop=True)
    df2 = group.copy()
    # iter over dataframe rows (iter on id_figlio)
    for i, _ in group.iterrows():
        # id_secondario, qta_secondario first dataframe
        id_figlio = group.iloc[i]['id_secondario']
        qta_figlio = group.iloc[i]['qta_totale']
        # print(f"Figlio {id_figlio} -> Quantità figlio {qta_figlio}")
        # iter over a copy of the same dataframe (iter on id_padre)
        for j, _ in df2.iterrows():
            id_padre = df2.iloc[j]['id_primario']
            qta_padre = df2.iloc[j]['qta_totale']
            # compare id_figlio -> id_padre
            if id_figlio == id_padre:
                #print(f"Figlio {id_figlio} -> Quantità figlio {qta_figlio}")
                #print(f"Padre {id_padre} -> quantità pre moltiplicazione {qta_padre}")
                qta_padre = qta_padre * qta_figlio
                # collect new dict((id_padre, id_figlio) = qta) -> key = (id_padre, id_figlio)
                qta_totale.update({(id_padre, df2.iloc[j]['id_secondario']) : qta_padre})
                #print(f"Padre {id_padre} -> quantità post moltiplicazione {qta_padre}")
            else:
                continue
    return qta_totale


def multiply_qta(group, qta_dict):
    concat_mult = []
    # multiply quantity filtered dataframe
    for k, v in qta_dict.items():
        filt = (group['id_primario'] == k[0]) & (group['id_secondario'] == k[1])
        mult = group[filt]
        if mult.empty:
            return False
        else:
            with ChainedAssignent():
                mult['qta_totale'] = v
                concat_mult.append(mult)
            continue

    concatenated = pd.concat(concat_mult)
    df_group = list(group.index.values)
    df_concatenated = list(concatenated.index.values)
    diff_index = [x for x in df_group if x not in df_concatenated]
    #missing_codes --> codici singoli + padri senza figli
    missing_codes = group.loc[diff_index]
    final = pd.concat([concatenated, missing_codes], axis=0, ignore_index=True)
    final.sort_index(inplace=True)
    return final
    
def main_mult_mrp(df):
    # change with argument dataframe -> test function 
    #df = read_distinta()

    qta_type = df['qta_totale'].dtype
    if isinstance(qta_type, float):
        with ChainedAssignent():
            df['qta_totale'] = df['qta_totale'].astype(float)
    else:
        with ChainedAssignent():
            df['qta_totale'] = df['qta_totale'].str.replace(',', '.').astype(float)
    df.index.name = 'id'
    
    artico_groups = group_artico(df)

    tables = []
    
    for name, group in artico_groups.items():
        qta_dict = extract_qta(group)
        if len(qta_dict.keys()) == 0:
            tables.append(group)
            continue

        result = multiply_qta(group, qta_dict)
        # add missing record from df to concatenated
        if result.empty:
            tables.append(group)
            continue
        else: 
            print("****************************")
            print(f"Moltiplicato codice {name}")
            print("****************************")
            tables.append(result)
            continue

    distinta_base = pd.concat(tables, axis=0)
    return distinta_base



if __name__ == '__main__':

    start = time.time()
    
    df = read_dataframe()
    #grouped_dataframe = group_dataframe(df)

    with concurrent.futures.ProcessPoolExecutor() as executor:
        result = executor.map(main_mult_mrp, group_dataframe(df))
        distinta_total = pd.concat(result, axis=0, ignore_index=True)
        distinta_total.to_pickle('mult_mrp.pkl')
        print("Distinta Moltiplicata")

        end = time.time() - start
        print(f"Process Time --> Minutes: {end / 60}, Seconds: {end}")