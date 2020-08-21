#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
    Взаимодействие с DaData, в т.ч. для полчения ИНН организаций.
"""


import json
import numpy as np
import pandas as pd
import requests


# ничего страшного, если ключ будет раскрыт, отдельный файл решил не создавать
API_KEY = '2091450bb5fcd63569c2cf4c315bc0d54bc52f22'

# ссылка (постоянная) для подсказок от DaData
URL = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/suggest/%s'

# класс "отвечателя" DaData
class DadataResponder:
    def __init__(self, api_key):
        self.API_KEY = api_key

    def suggest(self, query, resource):
        url = URL % resource
        headers = {
            'Authorization': 'Token %s' % self.API_KEY,
            'Content-Type': 'application/json',
        }
        data = { 'query': query }
        r = requests.post(url, data=json.dumps(data), headers=headers)
        return r.text

# некрасиво, но удобно, чтобы не объявлять global-блок
responder = DadataResponder(API_KEY)

def get_inn(x):
    '''
    Возвращает ИНН по склейке (+) наименования МРИГО и имени организации.

    Parameters:
        x (str):Склейка наименования МРИГО и имени организации.

    Returns:
        inn(str) или np.nan(float):ИНН организации, или метка, что он отсутствует.
    '''
    data = json.loads(responder.suggest(x, 'party'))['suggestions']
    if data:
        data_df = pd.io.json.json_normalize(data)
        return data_df['data.inn'].tolist()
    return np.nan

def get_inn_via_mrigo_name(df):
    '''
    Возвращает новый датафрейм с найденными ИНН для организаций.

    Parameters:
        df(pandas.DataFrame):Датафрейм с наименованием МРиГО организации и именем организации.

    Returns:
        df_(pandas.DataFrame):Новый датафрейм с дополнительным атрибутом с найденными ИНН ('inn').
    '''
    df_ = df.copy()
    df_['mrigo_name'] = df_.mrigo + ' ' + df_.name
    df_['inn_new'] = df_.mrigo_name.apply(get_inn)
    df_.drop(columns=['mrigo_name'], inplace=True)
    return df_