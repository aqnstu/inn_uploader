#!/usr/bin/env python3
# -*- coding: utf8 -*-
"""
    Скрипт для создания следующей таблицы:
        - organization_inn (id -- integer {PK}, id_organization -- integer {FK}, inn -- character varying, date_add -- timestamp without time zone)
    Загрузка в БД ias схему data.
"""


import numpy as np
import os
import pandas as pd
import re
import sqlalchemy as sa

import misc.api as api
import misc.db as db


def upload_organization_inn():
    # получаем таблицу с организациями
    organization_df = db.get_table_from_db_by_table_name('data.organization')

    # сортируем organization_df по id
    organization_df.sort_values(by=['id'], inplace=True)
    # устанавливаем новый индекс
    organization_df.set_index('id', inplace=True)

    # чистим наименования организаций
    organization_df.name = organization_df.name.str.strip()
    organization_df.name = organization_df.name.replace(r'\s+', ' ', regex=True)

    # чистим текущие ИНН организаций
    organization_df.inn = organization_df.inn.str.strip()
    organization_df.inn = organization_df.inn.replace(r'\s+', '', regex=True)
    organization_df.inn = organization_df.inn.apply(lambda x: None if len(x) < 10 else x)

    # получаем таблицу с наименованиями МРиГО и кодами
    mrigo_df = db.get_table_from_db_by_table_name('data.mrigo')
    mrigo_dict = dict(zip(mrigo_df.id.tolist(), mrigo_df.name.tolist()))

    # сопосталение кода МРиГО с фактическим наименованием
    organization_df['mrigo'] = organization_df.id_mrigo.apply(lambda x: mrigo_dict[x] if type(x) == int else None)

    # делаем списки из inn, чтобы потом применить explode
    organization_df.inn = organization_df.inn .apply(lambda x: [x] if type(x) == str else x)

    # сопоставление ИНН с помощью DaData
    organization_updated_df = api.get_inn_via_mrigo_name(organization_df)
    organization_updated_df.inn.fillna(organization_updated_df.inn_new, inplace=True)

    # создаем таблицу с ИНН
    inn_df = organization_updated_df[['inn']]
    inn_df = inn_df.explode('inn')
    inn_df.reset_index(inplace=True)
    inn_df.rename(columns={'id': 'id_organization'}, inplace=True)
    inn_df.dropna(subset=['inn'], inplace=True)

    # сохраним в таблицы .xlsx результаты
    organization_updated_df.reset_index(inplace=True)
    organization_updated_df.to_excel(os.path.join('tables', 'organization_updated.xlsx'), index=None, header=True)
    inn_df.to_excel(os.path.join('tables', 'inn.xlsx'), index=None, header=True)

    # выгружаем в БД таблицу с ИНН организаций из organization
    with open(os.path.join('sql', 'organization_inn.sql'), 'r') as f:
        organization_inn_query = sa.text(f.read())
    db.engine.execute(organization_inn_query)
    inn_df.to_sql(
        'organization_inn',
        con=db.engine,
        schema='data',
        if_exists='append',
        index=False,
        chunksize=None,
        method='multi',
    )


if __name__ == "__main__":
    upload_organization_inn()