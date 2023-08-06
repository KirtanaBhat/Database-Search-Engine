import pandas as pd 
import time
import sqlite3
import numpy as np
import warnings
import sys

from multiprocessing import Pool
import subprocess
# import mysql.connector
# import pymysql
# from sqlalchemy import create_engine, insert, Table, MetaData


warnings.filterwarnings(action='once')

 
# cnxn = sqlite3.connect("Basic.sqlite")
# curx = cnxn.cursor()

# conn = sqlite3.connect("Basic_New_1.sqlite")
# cur = conn.cursor()

# conn_2018 = sqlite3.connect("Basic_2018.sqlite")
# cur_2018 = conn_2018.cursor()

# conn_2019 = sqlite3.connect("Basic_2019.sqlite")
# cur_2019 = conn_2019.cursor()

# conn_2020 = sqlite3.connect("Basic_2020.sqlite")
# cur_2020 = conn_2020.cursor()

# conn_2021 = sqlite3.connect("Basic_2021.sqlite")
# cur_2021 = conn_2021.cursor()

# conn_2022 = sqlite3.connect("Basic_2022.sqlite")
# cur_2022 = conn_2022.cursor()

conn_2018 = sqlite3.connect("Databases/Basic_2018.sqlite", check_same_thread=False)
cur_2018 = conn_2018.cursor()

conn_2019 = sqlite3.connect("Databases/Basic_2019.sqlite", check_same_thread=False)
cur_2019 = conn_2019.cursor()

conn_2020 = sqlite3.connect("Databases/Basic_2020.sqlite", check_same_thread=False)
cur_2020 = conn_2020.cursor()

conn_2021 = sqlite3.connect("Databases/Basic_2021.sqlite", check_same_thread=False)
cur_2021 = conn_2021.cursor()

conn_2022 = sqlite3.connect("Databases/Basic_2022.sqlite", check_same_thread=False)
cur_2022 = conn_2022.cursor()

# mydb = mysql.connector.connect(
#   host="localhost",
#   user="root",
#   password="chirag12357"
# )
# mycursor = mydb.cursor()
# mycursor.execute("USE Basic")


# Connect to the database
# engine       = create_engine("mysql+pymysql://root:chirag12357@127.0.0.1/Basic")
# connection = engine.connect()


start_time = time.time()

# for i in range(2018, 2023):
#     exec_str = "create table importdata_A_{}(".format(str(i))
#     # exec_str = "create table importdata_A("

#     datatypes = []
#     count = 0
#     for i in curx.execute("""
#     pragma table_info(Data)
#     """).fetchall():
#         i = list(i)
#         if(i[2] == "REAL"):
#             datatypes += [np.float64]
#             i[2] = "float"
#         elif(i[2] == "TEXT"):
#             datatypes += [str]
#             i[2] = "nvarchar(255)"
#         elif(i[2] == "TIMESTAMP"):
#             datatypes += [pd._libs.tslibs.timestamps.Timestamp]
#             i[2] = "datetime"
#         else:
#             #skip the loop
#             continue
        

        
#         if(i[1] == "TOTAL_INSU_VALUE_ FORGN_CUR"):
#             exec_str += "TOTAL_INSU_VALUE_FORGN_CUR" + " " + i[2] + ", "
#     ##        exec_insert += "TOTAL_INSU_VALUE_FORGN_CUR, "
#         else:
#             exec_str += i[1] + " " + i[2] + ", "
#     ##        exec_insert += i[3] + ", "
#     exec_str = exec_str[:-2]
#     exec_str += ");"
#     mycursor.execute(exec_str)
#     mydb.commit()

# user_table = Table('importdata_A', MetaData())
# connection.execute(user_table.insert(), cur.execute("select * from importdata_A limit 1000").fetchall())

# exec_str_index = "create index search_index_sup on importdata_A(SUPPLIER_NAME)"
# cur.execute(exec_str)
# cur_2018.execute(exec_str)
# cur_2019.execute(exec_str)
# cur_2020.execute(exec_str)
# cur_2021.execute(exec_str)
# cur_2022.execute(exec_str)
# cur.execute(exec_str_index)

#exec_insert = exec_insert[:-2]
#exec_insert += ")\n"
# for chunk_dataframe in pd.read_sql_query("select * from importdata_A", cnxnn, chunksize=50000):
#     chunk_dataframe.rename({"TOTAL_INSU_VALUE_ FORGN_CUR" : "TOTAL_INSU_VALUE_FORGN_CUR"}, axis='columns', inplace = True)
#     # chunk_dataframe.to_sql(name='importdata_A', con=conn, if_exists='append', index = False)
#     chunk_dataframe.to_csv("Basic_New.csv", mode = "a")
# ##    exec_insert = "insert into importdata_A values\n"
# ##    for i in range(100):
# ##        exec_insert += "("
# ##        for j in range(68):
# ##            if(type(chunk_dataframe.loc[i][j]) != datatypes[j]):
# ##                exec_insert += "None, "
# ##            elif(type(chunk_dataframe.loc[i][j]) == str):
# ##                exec_insert += "'" + str(chunk_dataframe.loc[i][j]) + "', "
# ##            elif(type(chunk_dataframe.loc[i][j]) == pd._libs.tslibs.timestamps.Timestamp):
# ##                exec_insert += "'" + str(chunk_dataframe.loc[i][j].year) + "-" + str(chunk_dataframe.loc[i][j].month) + "-" + str(chunk_dataframe.loc[i][j].day) + "', "
# ##            else:
# ##                exec_insert += str(float(chunk_dataframe.loc[i][j])) + ", "
# ##        exec_insert = exec_insert[:-2]
# ##        exec_insert += "),\n"
# ##    exec_insert = exec_insert[:-2]
# ##    
# ##    cur.execute(exec_insert)
#     print("Time to execute " + str(t) +" : ", time.time() - start_time)
#     t+=1
#     start_time = time.time()
# t=1
# for chunk in pd.read_csv("Basic_New_1-001.csv", chunksize=50000, low_memory=False):
#     chunk.rename({"TOTAL_INSU_VALUE_ FORGN_CUR" : "TOTAL_INSU_VALUE_FORGN_CUR"}, axis='columns', inplace = True)
#     chunk.drop(chunk.columns[0], axis=1, inplace=True)
#     try:
#         exec_insert = "insert into importdata_A values\n"+str(chunk.values.tolist())[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#         mycursor.execute(exec_insert)
#         mydb.commit()
#     except Exception as E:
#         chunk_values = chunk.values.tolist()
#         val = str(chunk_values.pop(int(str(E).split()[-1])-1)).replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#         exec_insert = "insert into importdata_A values\n"+str(chunk_values)[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#         # print(int(str(E)[-1])-1)
#         try:
#             mycursor.execute(exec_insert)
#             mydb.commit()
#         except Exception as E:
#             print(E,t)
#             # exec_insert = "insert into importdata_A values\n"+str(chunk_values[11:49990])[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")

#             # print(str(chunk_values.pop(int(str(E)[-1])-1)).replace("None", "null").replace("[", "(").replace("]", ")"))
#             # for i in range(0, 50000, 10000):
#             #     try:
#             #         chunked = chunk_values[i:i+10000]
#             #         exec_insert = "insert into importdata_A values\n"+str(chunked)[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#             #         mycursor.execute(exec_insert)
#             #         mydb.commit()
#             #     except Exception as E:
#             #         print(E)
#             #         print("Error at ", i)
#             #         break

#     print("Time to execute " + str(t) +" : ", time.time() - start_time)
#     t+=1
#     start_time = time.time()

#
# a = cur.execute("select PRODUCT_DESCRIPTION from importdata_A where BEDATE between '2022-01-01' and '2022-12-31' limit 100000").fetchall()
# a = cur_2022.execute("select * from importdata_A limit 100000").fetchall()
# count = 0
# for chunk_dataframe in pd.read_sql_query('select count(distinct SUPPLIER_NAME) from importdata_A', conn, chunksize=100000):
#     print(len(chunk_dataframe))
#     count += len(chunk_dataframe)
#     print("Time to execute : ", time.time() - start_time)
#     start_time = time.time()

#create index on BE_DATE, SUPPLIER_NAME, IMPOERTER_NAME, PRODUCT_DESCRIPTION

# exec_index = "create index search_index_supdate on importdata_A(BEDATE, SUPPLIER_NAME)"
# cur_2019.execute(exec_index)

# start_time = time.time()
# exec_string = "select * from importdata_A indexed by search_index_sup where BEDATE between '2018-01-01' and '2018-07-01' and SUPPLIER_NAME like '%SCHNEIDER%'"
# cur_2018.execute(exec_string)
# print(len(cur_2018.fetchall()))
# print("Time to execute : ", time.time() - start_time)
# start_time = time.time()
# exec_string = "select * from importdata_A indexed by search_index_supdate where BEDATE between '2018-01-01' and '2018-07-01' and SUPPLIER_NAME like '%SCHNEIDER%'"
# cur_2018.execute(exec_string)
# print(len(cur_2018.fetchall()))
# print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_sup on importdata_A(SUPPLIER_NAME)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_imp on importdata_A(IMPORTER_NAME)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_prod on importdata_A(PRODUCT_DESCRIPTION)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_supdate on importdata_A(BEDATE, SUPPLIER_NAME)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_impdate on importdata_A(BEDATE, IMPORTER_NAME)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_proddate on importdata_A(BEDATE, PRODUCT_DESCRIPTION)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)

# for i in [cur_2018, cur_2019, cur_2020, cur_2021, cur_2022]:
#     start_time = time.time()
#     exec_index = "create index search_index_supimpproddate on importdata_A(BEDATE, SUPPLIER_NAME, IMPORTER_NAME, PRODuCT_DESCRIPTION)"
#     i.execute(exec_index)
#     print("Time to execute : ", time.time() - start_time)







# start_time = time.time()
# cur_2019.execute("select count(*) from importdata_A indexed by search_index_supindprod where SUPPLIER_NAME like '%SCHNEIDER%'  and IMPORTER_NAME like '%SCHNEIDER%' and PRODUCT_DESCRIPTION like '%CABLE%'")
# print(cur_2019.fetchall())
# print("Time to execute : ", time.time() - start_time)



# for i,j in [[cur_2018, "2018"], [cur_2019, "2019"], [cur_2020, "2020"], [cur_2021, "2021"], [cur_2022, "2022"]]:
#     start_time = time.time()
#     exec_index = "create index search_index on importdata_A(BEDATE, PRODUCT_DESCRIPTION, SUPPLIER_NAME, IMPORTER_NAME)"
#     i.execute(exec_index)
#     print("Time to execute {}".format(j) + " : ", time.time() - start_time)

    # t = 1
    # start_time = time.time()
    # for chunk_dataframe in pd.read_sql_query("select * from importdata_A", i, chunksize=50000):
    #     try:
    #         exec_insert = "insert into importdata_A_{} values\n".format(j)+str(chunk_dataframe.values.tolist())[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
    #         mycursor.execute(exec_insert)
    #         mydb.commit()
    #     except Exception as E:
    #         chunk_values = chunk_dataframe.values.tolist()
    #         val = str(chunk_values.pop(int(str(E).split()[-1])-1)).replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
    #         exec_insert = "insert into importdata_A_{} values\n".format(j)+str(chunk_values)[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
    #         try:
    #             mycursor.execute(exec_insert)
    #             mydb.commit()
    #         except Exception as E:
    #             print(E,t)
    #     print("Time to execute {}_".format(j) + str(t) +" : ", time.time() - start_time)
    #     t+=1
    #     start_time = time.time()
# print("reached")
# t = 0
# for chunk_dataframe in pd.read_sql_query("select * from importdata_A", conn, chunksize=10000):
#     chunk_dataframe.rename({"TOTAL_INSU_VALUE_ FORGN_CUR" : "TOTAL_INSU_VALUE_FORGN_CUR"}, axis='columns', inplace = True)
#     # chunk_dataframe.to_sql(name='importdata_A', con = engine, if_exists='append', index = False,)

#     try:
#         exec_insert = "insert into importdata_A values\n"+str(chunk_dataframe.values.tolist())[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#         mycursor.execute(exec_insert)
#         mydb.commit()
#     except:

#         exec_insert = "insert into importdata_A values\n"+str(chunk_dataframe.values.tolist()[0:2])[1:-1].replace("nan", "null").replace("None", "null").replace("[", "(").replace("]", ")")
#         print(exec_insert)
#         break
#         mycursor.execute(exec_insert)
#         mydb.commit()
#     print("Time to execute {}".format(t) + " : ", time.time() - start_time)
#     t+=1
#     start_time = time.time()


# limit = 10000
# offset = 0
# rows = cur.execute("select * from importdata_A limit {} offset {}".format(limit, offset)).fetchall()
# offset += limit
# exec_insert = "insert into importdata_A values\n"
# mycursor.execute(exec_insert+str(rows)[1:-1].replace("None", "null"))
# mydb.commit()
# while rows:
#     offset += limit
#     rows = cur.execute("select * from importdata_A limit {} offset {}".format(limit, offset)).fetchall()
#     exec_insert = "insert into importdata_A values\n"
#     print(rows[1])
#     mycursor.execute(exec_insert+str(rows)[1:-1].replace("None", "null"))
#     mydb.commit()
#     print("Time to execute " + str(offset) +" : ", time.time() - start_time)
#     start_time = time.time()

# print(mycursor.execute("select count(*) from importdata_A where SUPPLIER_NAME like '%%' limit 100").fetchall())

# print("Time to execute : ", time.time() - start_time)



start_date = input("Enter start date in YYYY-MM-DD format : ")
end_date = input("Enter end date in YYYY-MM-DD format : ")
supplier = input("Enter supplier name or leave blank for no name : ")
importer = input("Enter importer name or leave blank for no name : ")
product = input("Enter product name or leave blank for no name : ")

query_type = ""

if supplier == "":
    supplier = ""
    query_type += "0"
else:
    supplier = "SUPPLIER_NAME like '%"+supplier+"%' and"
    query_type += "1"

if importer == "":
    importer = ""
    query_type += "0"
else:
    importer = "IMPORTER_NAME like '%"+importer+"%' and"
    query_type += "1"

if product == "":
    product = ""
    query_type += "0"
else:
    product = "PRODUCT_DESCRIPTION like '%"+product+"%' and"
    query_type += "1"

indexes = {"100" : "search_index_sup", "010" : "search_index_imp", "001" : "search_index_prod", "110" : "search_index_supimpproddate", "101" : "search_index_supimpproddate", "011" : "search_index_supimpproddate", "111" : "search_index_supimpproddate"}

def create_exec(start, end, query_type, supplier, importer, product):
    print(query_type)
    if query_type in ["111", "110", "101", "011"]:
        exec_str = "select count(*) from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {} {} {}".format(indexes[query_type], start, end, supplier, importer, product[:-4])
    else:
        focus = ""
        for d in [supplier, importer, product]:
                if(len(d) != 0):
                    focus = d[:-4]
                    break
        print(focus)
        if(start[5:] == "01-01" and end[5:] == "12-31"):
            
                
            exec_str = "select * from importdata_A indexed by {} where {}".format(indexes[query_type],focus)
        else:
            exec_str = "select count(*) from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {}".format(indexes[query_type]+"date", start, end, focus)

    
    return exec_str

cursor = None
# if start_date[:4] == end_date[:4]:

#   exec("cursor = cur_"+start_date[:4])
#   exec_string = "select * from importdata_A where BEDATE between '{}' and '{}' and SUPPLIER_NAME like '{}' and IMPORTER_NAME like '{}' and PRODUCT_DESCRIPTION like '{}'".format(start_date, end_date, supplier, importer, product)
#   results = len(cursor.execute(exec_string).fetchall())
#   print(results)

# p = subprocess.Popen(["python3", "Python_Scripts/Run_2018.py", "Hello"])
# q = subprocess.Popen(["python3", "Python_Scripts/Run_2019.py", "World"])
# subprocess.Popen(["python3", "Python_Scripts/Run_2019.py"])
results = 0
start_time = time.time()

# def get_results(arguments):
#   return len(arguments[0].execute(arguments[1]).fetchall())

# args = []

cursor_dict = {"cur_2018": cur_2018, "cur_2019": cur_2019, "cur_2020": cur_2020, "cur_2021": cur_2021, "cur_2022": cur_2022}


for i in range(int(start_date[:4]), int(end_date[:4])+1):
    exec("cursor = cur_"+str(i))
    if(i == int(start_date[:4])):
        if(i == int(end_date[:4])):
            end = end_date
        else:
            end = str(i)+"-12-31"
        start = start_date

    elif(i == int(end_date[:4])):
        start = str(i)+"-01-01"
        end = end_date
    else:
        start = str(i)+"-01-01"
        end = str(i)+"-12-31"
    exec_string = create_exec(start, end, query_type, supplier, importer, product)
    print(f"""cur_{str(i)}.execute("{exec_string}").fetchall()[0]""")
    results = cursor_dict[f"cur_{str(i)}"].execute(exec_string).fetchall()
    # results += exec(f"""cur_{str(i)}.execute("{exec_string}").fetchall()[0]""")
print(len(results))


    
