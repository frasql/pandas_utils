import pandas as pd
from typing import List


# pulisce il dataframe prima delle operazioni
def clean_dafaframe(df):
    df = df.fillna(0)
    df['id padre'] == df['id padre'].astype(int)
    df['id figlio'] == df['id figlio'].astype(int)
    df = df.rename(columns={
        'Quantità': 'qta_totale', 
        'id padre': 'id_primario', 
        'padre': 'codice_primario',
        'Descrizione padre': 'descrizione_primario',
        'id figlio': 'id_secondario', 
        'figlio': 'codice_secondario',
        'Descrizione figlio': 'descrizione_secondario'
    })
    df['qta_totale'] = df['qta_totale'].str.replace(',', '.').astype(float)
    return df


class EsplodiCodice:
    """
    L'oggetto crea una lista di dataframe data dall'esplosione di un codice della distinta 
    """
    def __init__(self, df, code):
        assert isinstance(code, list)
        assert isinstance(df, pd.DataFrame)
        self.code = [int(x) for x in code]
        self.df = df
        self.index = 0
        self.values = []
        self.tables = []
        self.iterate()

    # consuma un valore dalla lista dei valori
    def consume_values(self) -> int:
        if len(self.values) > 0:
            return self.values.pop(0)
        else:
            print("End values")
 
    # genera un nuovo livello filtrando sull'ultimo valore se l'ultimo dataframe non è vuoto
    def generate_level(self) -> None:
        if len(self.tables) == 0: 
            filt = self.df['id_primario'].isin(self.code) 
            self.tables.append(self.df[filt])
        elif self.tables[-1].empty:
            raise StopIteration

        else:
            filt = self.df['id_primario'].isin(self.consume_values())
            self.tables.append(self.df[filt])
            self.index += 1

    # genera una nuova lista di valori (codici figli) da andare a ricercare nei padri per creare un nuovo livello
    def generate_values(self) -> List["int"]:
        if len(self.tables) > 0:
            vals = self.tables[self.index]['id_secondario'].values
            self.values.append(vals)
            return self.values
        else:
            pass

    # consuma un ogetto di tipo EsplodiCodici e ne ritorna una lista di dataframe dei suoi livelli + tappo (Empty DataFrame) --> [codice_base, primo_livello, ..., EmptyDataFrame]
    def iterate(self) -> None:
        while True:
            try:
                self.generate_level()
            except StopIteration:
                break
            self.generate_values()

    @property
    def get_esplosione(self) -> pd.DataFrame:
        return pd.concat(self.tables, axis=0, ignore_index=True)



class ManipolaEsplosione:
    def __init__(self, df_concat) -> None:
        assert isinstance(df_concat, pd.DataFrame)
        df_concat.index.name= 'id'
        self.concat_df = df_concat

    def merge_df(self):
        copy_df = self.concat_df.copy()
        joined = pd.merge(self.concat_df, copy_df, how='outer', left_on='id_primario', right_on='id_secondario')
        joined = joined.dropna()
        joined = joined.drop(columns=['codice_secondario_y', 'descrizione_secondario_y', 'id_secondario_y'])
        joined = joined[[
            'codice_primario_y',
            'descrizione_primario_y', 
            'id_primario_y', 
            'codice_primario_x', 
            'descrizione_primario_x', 
            'id_primario_x', 
            'qta_totale_y', 
            'id_secondario_x', 
            'codice_secondario_x', 
            'descrizione_secondario_x', 
            'qta_totale_x'
        ]]
        # joined['qta_molt'] = joined['qta_totale_y'] * joined['qta_totale_x']
        return joined

    def multiply(self):
        joined = self.merge_df()
        d = joined[['id_primario_x', 'qta_totale_y']]
        d.drop_duplicates(inplace=True)
        d_dict = d.to_dict(orient='records')
        list_var = []
        for _, val in enumerate(d_dict):
            var = joined.loc[joined['id_primario_y'] == val['id_primario_x']]
            var['qta_totale_y'] = var['qta_totale_y'] * val['qta_totale_y']
            list_var.append(var)

        list_var = pd.concat(list_var, axis=0, ignore_index=True)

        padri_drop = [item['id_primario_x'] for idx, item in enumerate(d_dict)]

        filt = joined['id_primario_y'].isin(padri_drop)
        joined = joined[~filt]
        final = pd.concat([joined, list_var], axis=0, ignore_index=True)
        final['qta_finale'] = final['qta_totale_y'] * final['qta_totale_x']

        return final
    

    def __repr__(self) -> 'str':
        return f"<ManipolaEsplosione(df_concat={self.concat_df})>"


if __name__ == '__main__':

    df = pd.read_csv('nuova_distinta.csv', sep=";", encoding='latin', low_memory=False)
    df = clean_dafaframe(df)
    
    list_codes = [3315, 3314, 60528]
    
    codici_moltiplicati = []

    for code in list_codes:
        distinta = EsplodiCodice(df, [code])
        manipulate = ManipolaEsplosione(distinta.get_esplosione)
        final = manipulate.multiply()
        final['id_artico'] = code
        codici_moltiplicati.append(final)

    print(codici_moltiplicati)
    


    

        




    