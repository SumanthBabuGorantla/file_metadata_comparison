import os

import psycopg2 as pg
import pandas as pd
import json
from pathlib import Path
import ydata_profiling as pf
from datetime import datetime
from deepdiff import DeepDiff
import tkinter
from tkinter import filedialog

root = tkinter.Tk()
root.withdraw()  # use to hide tkinter window


def search_for_file_path():
    currdir = os.getcwd()
    tempdir = filedialog.askopenfilenames(parent=root, initialdir=currdir, title='Please select a file to upload',
                                          filetypes=(("CSV Files", "*.csv"),))
    if len(tempdir) > 0:
        print(tempdir)
    return tempdir


def connect_to_db():
    con = pg.connect(database='demo', user='postgres', password='Suman@124'
                     , host='localhost', port='5432')
    cur = con.cursor()
    return cur, con


def close_connection(connection):
    connection.commit()
    connection.close()


def create_source_file_config(cursor):
    query = 'create table source_file_config(id serial NOT NULL PRIMARY KEY,uuid uuid,file_label varchar,file_format ' \
            'varchar,load_type char(15),source_file_location_pattern varchar,file_name_pattern varchar,' \
            'is_transformation_enabled boolean,target_file_name_pattern varchar,target_file_location_pattern varchar,' \
            'metadata json,file_stats json,glue_catalog_table_name varchar,job_id int,status boolean,business_process ' \
            'varchar,' \
            'mstr_mtd_mtch boolean,' \
            'version varchar,created_by int,' \
            'created_at timestamp,updated_at timestamp) '
    cursor.execute(query)


def create_master_metadata_table(cursor):
    query = 'create table master_metadata(id serial NOT NULL PRIMARY KEY,business_process varchar,file_name_pattern ' \
            'varchar, metadata json,version varchar,created_by int,' \
            'start_date timestamp,end_date timestamp) '
    cursor.execute(query)


def check_existence(curso, search_file, bp):
    query = "SELECT version "
    query += " FROM master_metadata WHERE  file_name_pattern LIKE %s  "
    query += " and end_date is null and business_process = %s ORDER BY id desc limit 1"
    curso.execute(query, [search_file + '%', bp])
    return curs.fetchall()


def insert_instance_metadata(cur, bp, file_name, m_data, s_data, version, match):
    query1 = """insert into source_file_config(metadata,file_stats,business_process,file_name_pattern,created_at,version,
    mstr_mtd_mtch) values(%s,%s,%s,%s,%s,%s,%s) """
    cur.execute(query1, (m_data, s_data, bp, file_name, datetime.now(), version, match))
    # cur.execute()


def insert_master_metadata(cur, bp, file_name, m_data, version):
    query1 = """insert into master_metadata(business_process,metadata,file_name_pattern,start_date,version) values(
    %s,%s,%s,%s,%s) """
    cur.execute(query1, (bp, m_data, file_name, datetime.now(), version))
    # cur.execute()


def compare_metadata(cur, search_filename, bp, version, mj):
    query = "SELECT metadata "
    query += " FROM master_metadata WHERE  file_name_pattern LIKE %s  "
    query += " and version = %s and business_process = %s ORDER BY id desc limit 1"
    cur.execute(query, [search_filename + '%', version[0][0], bp])
    meta_Data_list = cur.fetchall()
    # query1 = "SELECT metadata "
    # query1 += " FROM source_file_config WHERE  file_name_pattern LIKE %s  "
    # query1 += " ORDER BY id desc limit 1"
    # cur.execute(query1, [search_filename + '%'])
    # cur.execute(query1+query2)
    # meta_Data_list_cur = cur.fetchall()
    diff = DeepDiff(meta_Data_list[0][0], mj, ignore_order=True)
    return diff


def generate_metadata(file_name):
    df = pd.read_csv(file_name)
    nul_cols = dict(df[df.columns[df.isnull().any()]].isnull().sum())
    profile = pf.ProfileReport(df, title="profiling report")
    json_data = profile.to_json()
    data = json.loads(json_data)
    meta_data = []
    col_stats = []
    for col in df.columns:
        m_dict = {'Column Name': col, 'Type': data['variables'][col]['type'],
                  "Is Unique": data['variables'][col]["is_unique"]}
        if data['variables'][col]["n_missing"] == 0:
            m_dict['Is Required'] = True
        else:
            m_dict['Is Required'] = True
        stats = {'Column Name': col, 'Null Count': data['variables'][col]["n_missing"]}
        if data['variables'][col]['type'] == 'Numeric' or data['variables'][col]['type'] == 'DateTime':
            stats['Min'] = data['variables'][col]["min"]
            stats['Max'] = data['variables'][col]["max"]
            if data['variables'][col]['type'] == 'DateTime':
                stats['Range'] = data['variables'][col]["range"]
            else:
                stats['Mean'] = data['variables'][col]["mean"]
        else:
            stats["max_length"] = data['variables'][col]["max_length"]
            stats["min_length"] = data['variables'][col]["min_length"]
            stats["mean_length"] = data['variables'][col]["mean_length"]
        col_stats.append(stats)
        meta_data.append(m_dict)
    return meta_data, col_stats


# file = r"C:\Users\chaitanya.thungani\Pictures\data\Orders_08062023.csv"
file = search_for_file_path()[0]
base_file = os.path.basename(file)
file_we = Path(file).resolve().stem.split('_')[0]
business_process = "Q1"
mdata, sdata = generate_metadata(file)
mdata_json = json.dumps(mdata, indent=4)
stats_json = json.dumps(sdata, indent=4)
curs, conn = connect_to_db()
# create_source_file_config(curs)
# create_master_metadata_table(curs)
existed_ver = check_existence(curs, file_we, business_process)
cmp_version = 1
matched = True
if len(existed_ver) == 0:
    insert_master_metadata(curs, business_process, base_file, mdata_json, 1)
else:
    print(f"the file already existed with version:{existed_ver[0][0]}")
    cmp_version = existed_ver[0][0]
    is_diff = compare_metadata(curs, file_we, business_process, existed_ver, mdata)
    print(is_diff)
    matched = not bool(is_diff)
insert_instance_metadata(curs, business_process, base_file, mdata_json, stats_json, cmp_version, matched)

close_connection(conn)
