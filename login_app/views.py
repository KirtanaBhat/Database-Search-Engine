from django.shortcuts import render, redirect
from django.http import HttpResponse
from django.contrib.auth.models import User
from django.contrib.auth import authenticate, login, logout
from django.contrib import messages
from django.contrib.auth.decorators import login_required
import sqlite3
import pandas as pd
import time
import csv
from xlsxwriter.workbook import Workbook
import json
import polars as pl

conn_2018 = sqlite3.connect("Databases/Basic_2018.sqlite", check_same_thread=False)
cur_2018 = conn_2018.cursor()
str_2018 = "sqlite://Databases/Basic_2018.sqlite"

conn_2019 = sqlite3.connect("Databases/Basic_2019.sqlite", check_same_thread=False)
cur_2019 = conn_2019.cursor()
str_2019 = "sqlite://Databases/Basic_2019.sqlite"

conn_2020 = sqlite3.connect("Databases/Basic_2020.sqlite", check_same_thread=False)
cur_2020 = conn_2020.cursor()
str_2020 = "sqlite://Databases/Basic_2020.sqlite"

conn_2021 = sqlite3.connect("Databases/Basic_2021.sqlite", check_same_thread=False)
cur_2021 = conn_2021.cursor()
str_2021 = "sqlite://Databases/Basic_2021.sqlite"

conn_2022 = sqlite3.connect("Databases/Basic_2022.sqlite", check_same_thread=False)
cur_2022 = conn_2022.cursor()
str_2022 = "sqlite://Databases/Basic_2022.sqlite"

cursor_dict = {"cur_2018": cur_2018, "cur_2019": cur_2019, "cur_2020": cur_2020, "cur_2021": cur_2021, "cur_2022": cur_2022}
conn_dict = {"conn_2018": conn_2018, "conn_2019": conn_2019, "conn_2020": conn_2020, "conn_2021": conn_2021, "conn_2022": conn_2022}
str_dict = {"str_2018": str_2018, "str_2019": str_2019, "str_2020": str_2020, "str_2021": str_2021, "str_2022": str_2022}

def loginPage(request):
    fail = 0
    if request.user.is_authenticated:
        return redirect('/search')
    if request.method == "POST":
        uname = request.POST.get('uname')
        psw = request.POST.get('psw')

        try:
            user = User.objects.get(username=uname)
        except:
            messages.error(request, 'User does not exist')

        user = authenticate(request, username=uname, password=psw)

        if user is not None:
            login(request, user)
            # return render(request, 'login/Search-page.html')
            return redirect('/search')
        else:
            fail = 1
            messages.error(request, 'Username OR password is incorrect')
    context = {"fail": fail}
    return render(request, 'login/login.html', context)

def create_exec(start, end, query_type, supplier, importer, product):

    cols = "BE_NO, STRFTIME('%d-%m-%Y', BEDATE) as date, HS_CODE, PRODUCT_DESCRIPTION, QUANTITY, UNIT, ASSESS_VALUE_INR, UNIT_PRICE_INR, ASSESS_VALUE_USD, UNIT_PRICE_USD, TOTAL_DUTY, TOTAL_DUTY_BE_WISE, APPLICABLE_DUTY_INR, EXCHANGE_RATE_USD, ITEM_RATE_INV_CURR, VALUE_INV_CURR, INVOICE_CURRENCY, ASSESS_GROUP, IMPORTER_CODE, IMPORTER_NAME, IMPORTER_ADDRESS, IMPORTER_CITY, IMPORTER_PIN, IMPORTER_STATE, SUPPLIER_CODE, SUPPLIER_NAME, SUPPLIER_ADDRESS, SUPPLIER_COUNTRY, FOREIGN_PORT, FOREIGN_COUNTRY, FOREIGN_REGIONS, CHA_NAME, CHA_PAN, IEC, IEC_CODE, INVOICE_NUMBER, INVOICE_SR_NO, ITEM_NUMBER, HSCODE_2DIGIT, HSCODE_4DIGIT, TYPE, INDIAN_PORT, SHIPMENT_MODE, INDIAN_REGIONS, SHIPMENT_PORT, HSCODE_6DIGIT, BCD_NOTN, BCD_RATE, BCD_AMOUNT_INR, CVD_NOTN, CVD_RATE, CVD_AMOUNT_INR, IGST_AMOUNT_INR, GST_CESS_AMOUNT_INR, REMARK, INCOTERMS, TOTAL_FREIGHT_VALUE_FORGN_CUR, FREIGHT_CURRENCY, TOTAL_INSU_VALUE_FORGN_CUR, INSURANCE_CURRENCY, TOTAL_INVOICE_VALUE_INR, INSURANCE_VALUE_INR, TOTAL_GROSS_WEIGHT, TOTAL_FREIGHT_VALUE_INR, GROSS_WEIGHT_UNIT, CUSTOM_NOTIFICATION, STANDARD_QUANTITY, STANDARD_QUANTITY_UNIT"
    # cols = "STRFTIME('%d-%m-%Y', BEDATE) as date"
    indexes = {"100" : "search_index_sup", "010" : "search_index_imp", "001" : "search_index_prod", "110" : "search_index_supimpproddate", "101" : "search_index_supimpproddate", "011" : "search_index_supimpproddate", "111" : "search_index_supimpproddate"}

    if query_type in ["111", "110", "101", "011"]:
        exec_str = "select {} from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {} {} {}".format(cols, indexes[query_type], start, end, supplier, importer, product[:-4])
    else:
        focus = ""
        for d in [supplier, importer, product]:
                if(len(d) != 0):
                    focus = d[:-4]
                    break
        if(start[5:] == "01-01" and end[5:] == "12-31"):
            
                
            exec_str = "select {} from importdata_A indexed by {} where {}".format(cols, indexes[query_type],focus)
        else:
            exec_str = "select {} from importdata_A indexed by {} where BEDATE between '{}' and '{}' and {}".format(cols, indexes[query_type]+"date", start, end, focus)

    
    return exec_str

@login_required(login_url='login')
def search(request):

    # print(time.time() - start_time)


    if request.method == "POST":
        start_date = request.POST.get('from_date').upper()
        end_date = request.POST.get('to_date').upper()
        supplier = request.POST.get('SN').upper()
        importer = request.POST.get('IN').upper()
        product = request.POST.get('PD').upper()

        df = pd.DataFrame()
        dl = pl.DataFrame()
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

        
        if start_date == "" or int(start_date[:4]) < 2018:
            start_date = "2018-01-01"

        if end_date == "" or int(end_date[:4]) > 2022:
            end_date = "2022-12-31"
        cursor = None

        results = 0
        # start_time = time.time()
        writer = pd.ExcelWriter('Databases/Results.xlsx', engine='xlsxwriter')


        for i in range(int(start_date[:4]), int(end_date[:4])+1):
            conn = conn_dict["conn_"+str(i)]
            string = str_dict["str_"+str(i)]
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
            # print(exec_string)
            
            # results = cursor_dict[f"cur_{str(i)}"].execute(exec_string).fetchall()
            # print(len(results))
            start_time = time.time()
            # for chunk_dataframe in pd.read_sql_query(exec_string, conn, chunksize=10000):
            #     print("Time to read from sql : ", time.time() - start_time)

            #     start_time = time.time()
                
            #     data = [df, chunk_dataframe]
            #     df = pd.concat(data, ignore_index=True)
            #     # print(len(chunk_dataframe))
            #     print("Time to convert to df : ",time.time() - start_time)
            #     start_time = time.time()
            
            start_time = time.time()
            data = [dl, pl.read_sql(exec_string, string)]
            dl = pl.concat(data)

            # data = [df, pd.read_sql(exec_string, conn)]
            # df = pd.concat(data, ignore_index=True)
            print("Time to convert to df : ",time.time() - start_time)
       

        start_time1 = time.time()
        # df.to_csv("Databases/Results.csv", index=False)
        dl.write_csv("Databases/Results.csv")
        # df.to_excel(writer, sheet_name='Sheet1', index=False)
        # # worksheet = writer.sheets['Sheet1']

        
        
        
        # with Workbook('Databases/Results.xlsx') as workbook:
        #     dl.write_excel(workbook=workbook)

        csvfile = "Databases/Results.csv"
        workbook = Workbook(csvfile[:-4] + 'converted.xlsx')
        worksheet = workbook.add_worksheet()
        with open(csvfile, 'rt', encoding='utf8') as f:
            reader = csv.reader(f)
            for r, row in enumerate(reader):
                for c, col in enumerate(row):
                    worksheet.write(r, c, col)
        worksheet.autofit()
        worksheet.autofilter('A1:BP1')
        worksheet.freeze_panes(1, 0)
        workbook.close()

        print("Time to convert to excel : ",time.time() - start_time1)
        
        context = {'data': dl.iter_rows, "cols": dl.columns}
        # return render(request, 'login/Results.html', context)
        with open("Databases/Resultsconverted.xlsx", "rb") as f:
            response = HttpResponse(f.read(), content_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet')
            response['Content-Disposition'] = 'attachment; filename="Results.xlsx"'
            return response


    # context = {"today" : datetime.today().strftime('%Y-%m-%d')}
    context = {"today" : "2022-12-31"}
    return render(request, 'login/Search-page.html')

def logoutUser(request):
     logout(request)
     return redirect('/login')


