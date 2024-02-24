
import os
import numpy as np

import pandas as pd

# import matplotlib.pyplot as plt

# import seaborn as sns

# sns.set_context("paper", font_scale = 1.4)

from plotly import express as exp, graph_objects as go, io as pio

from plotly.subplots import make_subplots

import pymongo

import pickle

import datetime as dt

import warnings

import time as tm

import dask.dataframe as dd

from dotenv import load_dotenv

from functools import reduce

import requests

load_dotenv(".env")


pio.templates.default = 'ggplot2'

warnings.filterwarnings("ignore", category=DeprecationWarning)

warnings.filterwarnings("ignore", category=FutureWarning)


def main_func():

    # def get_data():

    # 	ddf = dd.read_csv(

    # 		"/content/drive/MyDrive/Projects/cin_crime_data_2019_2024/data/cin_crime_data.csv",

    # 		blocksize = 15e6,

    # 		dtype={

    # 				'address_x': "string",
    # 				'agency': "string",
    # 				'create_time_incident': "string",
    # 				'disposition_text': "string",
    # 				'event_number': "string",
    # 				'incident_type_id': "string",
    # 				'incident_type_desc': "string",
    # 				'priority': "string",
    # 				'priority_color': "string",
    # 				'closed_time_incident': "string",
    # 				'beat': "string",
    # 				'district': "string",
    # 				'sna_neighborhood': "string",
    # 				'cpd_neighborhood': "string",
    # 				'community_council_neighborhood': "string",
    # 				'latitude_x': "string",
    # 				'longitude_x': "string",
    # 				'arrival_time_primary_unit': "string",
    # 				'dispatch_time_primary_unit': "string",

    # 		}

    # 	)

    # 	return ddf


    def get_data():


        url = os.getenv('url')

        start = dt.datetime.strftime(dt.datetime(
            2019, 1, 1, 0, 0, 0), "%Y-%m-%d").replace("'", "")

        now = dt.datetime.strftime(
            dt.datetime.now(), "%Y-%m-%d").replace("'", "")

        rge = pd.date_range(start=start, end=now, freq='1M')

        rge = [dt.datetime.strftime(
            i, "%Y-%m-%d").replace("'", "") for i in rge]

        rge[0] = start

        rge[-1] = now

        datepairs = [[rge[i], rge[i + 1]] if i !=
                     len(rge) - 1 else [rge[i], now] for i in range(len(rge))]

        if datepairs[-2][1] == datepairs[-1][0]:

            datepairs = datepairs[:-1:1]

        for i in range(len(datepairs)):

            if i != 0:

                datepairs[i][0] = dt.datetime.strftime(

                    dt.datetime.strptime(datepairs[i][0],

                                         "%Y-%m-%d"

                                         ) + dt.timedelta(days=1),

                    "%Y-%m-%d"

                ).replace("'", "")

        queries = [
            f"$where=create_time_incident>='{pair[0]}T00:00:00.000' and create_time_incident<='{pair[1]}T23:59:59.000'&$limit=1000000000000" for pair in datepairs]

        # query = f"$where=create_time_incident>='2019-01-01T00:00:00.000' and create_time_incident<='{now}T23:59:59.000'&$limit=1000000000000"

        # query = "$limit=5"

        # response = requests.get(url, query, headers = headers)

        # jsn = response.json()

        # # df = pd.DataFrame(jsn)

        # # df.to_csv("./data/cin_crime_data.csv", index = False)

        user_agent = os.getenv('user_agent')

        headers = {


            "User-Agent": user_agent


        }


        # jsons = []


        # _ = pd.DataFrame()


        _ = dd.from_pandas(pd.DataFrame(), chunksize = 35000)


        for query in queries:


            response = requests.get(url, query, headers=headers)


            json = response.json()


            _pd = pd.DataFrame(json)


            _pd = _pd.reset_index(drop = True)


            _pd = dd.from_pandas(_pd, chunksize = 10000)


            _ = dd.concat([_, _pd], axis = 0)


            # print(f"\n\n{_.shape[0]}\n\n")



            # jsons.extend(json)

            # print(f"\n\n{len(jsons)}\n\n")

        # with open("./data/pickle/cin_json.pickle", 'wb') as pickle_file:

        #     pickle.dump(jsons, pickle_file, protocol = pickle.HIGHEST_PROTOCOL)



        return _




    ddf = get_data()


    print(ddf)

    print(ddf.columns)

    print(ddf.dtypes)

    print(ddf.npartitions)

    # ddf.head(n = 3)



    def show_nan(nan_criteria='nan'):

        ddf_len = ddf.sna_neighborhood.isna().sum().compute() + \
            ddf.sna_neighborhood.count().compute()

        nan_dict = {
            k: v for k, v in zip(
                ddf.columns, [
                    round(
                        (ddf[c].isna().sum().compute() / ddf_len) * 100, 1) for c in ddf.columns])}

        nan_df = pd.DataFrame()

        nan_df['column'] = list(nan_dict.keys())

        nan_df['nan_%'] = list(nan_dict.values())

        if 'nan' not in str(nan_criteria).lower():

            nan_df = nan_df[nan_df['nan_%'] >= nan_criteria]

        fig = exp.bar(nan_df, x='nan_%', y='column')

        fig.update_layout(bargap=0.32)

        fig.show()

        return None

    # show_nan()

    def get_value_counts_func(df):

        # cols = df.columns

        cols = ['event_number']

        counts_dict = {k: v for k, v in zip(

            cols,

            [ddf[c].value_counts().compute() for c in cols]


        )}

        return counts_dict

    value_counts_dict = get_value_counts_func(ddf)

    # value_counts_dict['event_number']

    timestamp_cols = [i for i in ddf.columns if 'time' in i.lower()]

    for t in timestamp_cols:

        ddf[t] = ddf[t].map_partitions(
            pd.to_datetime,
            format='%Y-%m-%dT%H:%M:%S.%f',
            meta=('datetime64[ns]'))

    # for c in ['district', 'priority']:

    #     ddf[c] == ddf[c].map_partitions(pd.to_numeric, errors='coerce')

    # ddf.head(2)

    ddf['create_closed_timedelta'] = ((

        ddf.closed_time_incident - ddf.create_time_incident

    ).dt.total_seconds() / 60).round(2)

    ddf['create_dispatch_timedelta'] = ((

        ddf.dispatch_time_primary_unit - ddf.create_time_incident

    ).dt.total_seconds() / 60).round(2)

    ddf['create_arrival_timedelta'] = ((

        ddf.arrival_time_primary_unit - ddf.create_time_incident

    ).dt.total_seconds() / 60).round(2)

    ddf['dispatch_arrival_timedelta'] = ((

        ddf.arrival_time_primary_unit - ddf.dispatch_time_primary_unit

    ).dt.total_seconds() / 60).round(2)

    # ddf.head(n = 2)

    ddf = ddf.drop(columns=['sna_neighborhood'])

    # ddf.head(2)

    def get_month(x):

        return x.apply(lambda y: y.month)

    def get_year(x):

        return x.apply(lambda y: y.year)

    def get_day(x):

        return x.apply(lambda y: y.day)

    def get_hour(x):

        return x.apply(lambda y: y.hour)

    ddf['create_time_incident_year'] = ddf.create_time_incident.map_partitions(
        get_year)

    ddf['create_time_incident_month'] = ddf.create_time_incident.map_partitions(
        get_month)

    ddf['create_time_incident_day'] = ddf.create_time_incident.map_partitions(
        get_day)

    ddf['create_time_incident_hour'] = ddf.create_time_incident.map_partitions(
        get_hour)

    from functools import reduce

    def get_nan_pivot(data, nancol, index, columns, values):

        def create_pivot_table(df):

            def count_unique(dframe):

                return dframe.nunique()

            # index.append(columns)

            grouped = df[df[nancol].isna()].groupby(

                index, dropna=False

            )

            pivot_table = grouped.agg({values: count_unique})

            return pivot_table.stack()

        years = data[columns].unique()

        year_dict = {k: v for k, v in zip(years, ["" for year in years])}

        for year in years:

            _ = pd.DataFrame(data[data[columns] == year].map_partitions(
                create_pivot_table).compute()).reset_index()

            cols = []

            cols.extend(index)

            cols.extend(['level_2', str(year)])

            _.columns = cols

            _ = _.drop(columns=['level_2'])

            year_dict[year] = _

        merge_on = []

        merge_on.extend(index)

        def merge_dfs(df1, df2):

            return pd.merge(df1, df2, on=merge_on, how='left')

        list_of_dfs = list(year_dict.values())

        merged_df = reduce(merge_dfs, list_of_dfs)

        return merged_df
        

    # ptable = get_nan_pivot(ddf, 'cpd_neighborhood', ['create_time_incident_day', 'create_time_incident_month'], 'create_time_incident_year', 'event_number')

    # eventnum = pd.DataFrame(value_counts_dict['event_number'])

 		# eventnum.columns = ['event_number']

    # r_eventnums = eventnum[eventnum.event_number == 2].index.values

    # ddf_enum = ddf[ddf.event_number.isin(r_eventnums)]

    # ddf_enum = ddf_enum.sort_values(
    #     by=['event_number', 'create_time_incident'], ascending=True)

    # ddf_enum[['address_x', 'agency', 'create_time_incident', 'disposition_text',
    #        'event_number', 'incident_type_id', 'incident_type_desc', 'priority',
    #        'priority_color', 'closed_time_incident', 'beat', 'district',
    #        'cpd_neighborhood', 'community_council_neighborhood', 'latitude_x',
    #        'longitude_x', 'arrival_time_primary_unit',
    #        'dispatch_time_primary_unit']].head(30)


    ddf = ddf.sort_values(
        by=['event_number', 'create_time_incident'], ascending=True)

    # ddf[ddf.event_number == 'CPD210819001652'].compute()

    ddf = ddf.reset_index(drop=False)

    ddf.columns = [
        'ind',
        'address_x',
        'agency',
        'create_time_incident',
        'disposition_text',
        'event_number',
        'incident_type_id',
        'incident_type_desc',
        'priority',
        'priority_color',
        'closed_time_incident',
        'beat',
        'district',
        'cpd_neighborhood',
        'community_council_neighborhood',
        'latitude_x',
        'longitude_x',
        'arrival_time_primary_unit',
        'dispatch_time_primary_unit',
        'create_closed_timedelta',
        'create_dispatch_timedelta',
        'create_arrival_timedelta',
        'dispatch_arrival_timedelta',
        'create_time_incident_year',
        'create_time_incident_month',
        'create_time_incident_day',
        'create_time_incident_hour']

    def enum_func(df):

        grouper = df[~df.district.isna()].groupby(
            ['event_number'], as_index=False, dropna=False).agg({"ind": pd.Series.max})

        return grouper

    ddf_ = ddf.map_partitions(enum_func)

    ddf_['enum_ind'] = ddf_.apply(lambda x: x[0] + ', ' + str(x[1]), axis=1)

    ddf_ = ddf_.drop(columns=['event_number', 'ind'])

    ddf['enum_ind'] = ddf.apply(lambda x: str(x[5]) + ', ' + str(x[0]), axis=1)

    # ddf.head(2)

    ddf_new = dd.merge(ddf_, ddf, on='enum_ind', how='inner')

    # ddf_new.head(5)

    # ddf_new[['address_x', 'agency', 'create_time_incident', 'disposition_text',
    #        'event_number', 'incident_type_id', 'incident_type_desc', 'priority',
    #        'priority_color', 'closed_time_incident', 'beat', 'district',
    #        'cpd_neighborhood', 'community_council_neighborhood', 'latitude_x',
    #        'longitude_x', 'arrival_time_primary_unit',
    #        'dispatch_time_primary_unit', 'ind']][ddf_new.event_number == 'CPD210819001652'].compute()

    ddf_new = ddf_new[['address_x',
                       'agency',
                       'create_time_incident',
                       'disposition_text',
                       'event_number',
                       'incident_type_id',
                       'incident_type_desc',
                       'priority',
                       'priority_color',
                       'closed_time_incident',
                       'beat',
                       'district',
                       'cpd_neighborhood',
                       'community_council_neighborhood',
                       'latitude_x',
                       'longitude_x',
                       'arrival_time_primary_unit',
                       'dispatch_time_primary_unit',
                       'create_closed_timedelta',
                       'create_dispatch_timedelta',
                       'create_arrival_timedelta',
                       'dispatch_arrival_timedelta',
                       'create_time_incident_year',
                       'create_time_incident_month',
                       'create_time_incident_day',
                       'create_time_incident_hour']]

    # Aggregating our dataframe

    def get_agg(data):

        def get_unique(dframe):

            return dframe.nunique()

        _df = data.groupby(

            [

                'create_time_incident_year',

                'create_time_incident_month',

                'create_time_incident_day',

                'create_time_incident_hour',

                'address_x',

                'disposition_text',

                'incident_type_id',

                'priority',

                'beat',

                'district',

                'cpd_neighborhood',

                'community_council_neighborhood',

                'latitude_x',

                'longitude_x'

            ],

            as_index=False,

            dropna=False


        ).agg(

            {

                "event_number": get_unique,

                "create_closed_timedelta": 'mean',

                "create_dispatch_timedelta": 'mean',

                "create_arrival_timedelta": 'mean',

                "dispatch_arrival_timedelta": 'mean'


            }


        )

        return _df

    agg_df = ddf_new.map_partitions(get_agg)

    # agg_df.head(n = 3)

    # agg_df.create_time_incident_year.isna().sum().compute() + agg_df.create_time_incident_year.count().compute()

    # Unique Vals Dict

    cols = agg_df.columns

    unique_dict = {k: v for k, v in zip(cols, ["" for c in cols])}

    for col in cols:

        unique_vals = agg_df[col].compute().unique()

        _dict = {
            "id": [
                i for i in range(
                    len(unique_vals))],
            col: list(unique_vals)}

        _ = dd.from_dict(_dict, npartitions=10)

        unique_dict[col] = _

    # PyMongo Section

    def open_connection():

        connection_string = os.getenv('mongodb_url')

        client = None

        try:

            client = pymongo.MongoClient(connection_string)

            client.admin.command('ping')

            print("Pinged your deployment. You successfully connected to MongoDB!")

        except BaseException as e:

            print(e)

            client = None

        return client

    def db_insert(data, dbname, collname):

        counter = 0

        while True:

            try:

                client = open_connection()

                if client:

                    db = client[dbname]

                    coll = db[collname]

                    if db is not None:

                        all_colls = db.list_collection_names()

                        if len(all_cols) > 0:

                            for c in all_colls:

                                db[c].drop()          # Drop the collection

                    # if collname in db.list_collection_names():

                    #   c = db[collname]

                    #   if c == None:

                    #     c.drop()

                    print("\nPreparing to insert data.\n")

                    # Write data to collection

                    def convert_to_dict(df):

                        return df.to_dict(orient='records')

                        data = data.map_partitions(convert_to_dict).compute()

                    if len(data) > 1:

                        coll.insert_many(data.to_dict(orient='records'))

                    elif len(data) == 1:

                        coll.insert_one(data.to_dict(orient='records'))

                    else:

                        pass

                    break        # Break out of the while loop

                else:

                    if counter > 5:

                        break

                    else:

                        print("Will retry after 1 minutes.")

                        tm.sleep(60)

                        counter += 1

                        continue

            except BaseException as e:

                print(e)

                if counter > 5:

                    break

                else:

                    tm.sleep(60)

                    print("Will retry after 1 minutes.")

                    counter += 1

                    continue

    def db_insert_mappings(data, dbname, collname):

        counter = 0

        while True:

            try:

                client = open_connection()

                if client:

                    db = client[dbname]

                    coll = db[collname]

                    if collname in db.list_collection_names():

                        c = db[collname]

                        if c is None:

                            c.drop()

                    print("\nPreparing to insert data.\n")

                    # Write data to collection

                    coll.insert_one(data)

                    # if len(data) > 1:

                    #   coll.insert_many(data.to_dict(orient = 'records'))

                    # elif len(data) == 1:

                    #   coll.insert_one(data.to_dict(orient = 'records'))

                    # else:

                    #   pass

                    break        # Break out of the while loop

                else:

                    if counter > 5:

                        break

                    else:

                        print("Will retry after 1 minutes.")

                        tm.sleep(60)

                        counter += 1

                        continue

            except BaseException as e:

                print(e)

                if counter > 5:

                    break

                else:

                    tm.sleep(60)

                    print("Will retry after 1 minutes.")

                    counter += 1

                    continue

            # def insert_mappings(dbname):

            #   collnames = list(distinct_cats.keys())

            #   datasets = list(distinct_cats.values())

            #   for i in range(len(collnames)):

            #     db_insert(data = datasets[i], dbname = dbname, collname = collnames[i])

    def db_get(dbname, collname):

        counter = 0

        while True:

            try:

                client = open_connection()

                if client:

                    db = client[dbname]

                    coll = db[collname]

                    if coll.count_documents({}) > 0:

                        return list(coll.find({}, projection={"_id": False}))

                else:

                    if counter > 5:

                        break

                    else:

                        print("Retrying after 1 minutes.")

                        tm.sleep(60)

                        counter += 1

                        continue

            except BaseException as e:

                print(e)

                if counter > 5:

                    break

                else:

                    print("Retrying after 1 minutes.")

                    tm.sleep(60)

                    counter += 1

                    continue

    def drop_collections(dbname):

        flag = False

        counter = 0

        while True:

            try:

                client = open_connection()

                if client[dbname] is not None:

                    flag = True

                if flag:

                    db = client[dbname]

                    colls = db.list_collection_names()

                    if len(colls) > 0:

                        for col in colls:

                            db[col].drop()          # Drop the collection

                else:

                    if counter > 5:

                        break

                    else:

                        counter += 1

                        tm.sleep(60)

                        continue

            except BaseException as e:

                print(f"\n\n{e}\n\n")

                if counter > 5:

                    break

                else:

                    counter += 1

                    tm.sleep(60)

                    continue

            else:

                print("\n\nDropped all collections.\n\n")

                break

    drop_collections(os.getenv('mongo_db_mappings_dbname'))

    drop_collections(os.getenv('mongodb_dbname'))

    db_insert(
        agg_df,
        os.getenv("mongodb_dbname"),
        os.getenv("mongodb_collname"))

    unique_dframes = list(unique_dict.items())

    for i in unique_dframes:

        db_insert(i[1], os.getenv("mongo_db_mappings_dbname"), i[0])

    # dframes = list(df_dict.items())

    # for dframe in dframes:

    #   db_insert(dframe[1], os.getenv('mongodb_dbname'), dframe[0])

    # for i in categorical_cols:

    #   db_insert(cat_col_vals_dict[i], os.getenv('mongo_db_mappings_dbname'), i)

    print("\n\nData Insertion Done Successfully!\n\n")



if __name__ == "__main__":


    main_func()
