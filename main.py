from flask import Flask
from flask import Flask,render_template,request
import os
from os import listdir

import os,os.path
from os.path import isfile, join
import csv
import time
import random
import sys
from flask import Flask
#from azure.storage.blob import BlockBlobService
#from azure.storage.blob import ContentSettings
from flask import render_template
from flask import request
import mysql.connector
#from mysql.connector import errorcode
#import pyodbc
import pymysql
import base64
import datetime
import time
#import uuid
#from azure.storage.blob import PublicAccess
#import pymysql
from flask import Flask, render_template, session, request, flash, redirect, url_for

# Obtain connection string information from the portal
#config = {
# server ='aishdb.database.windows.net'
# username ='aish'
# password ='Qwerty123'
# database ='AishDb'
#driver= '{ODBC Driver 13 for SQL Server}'
#cnxn = pyodbc.connect('DRIVER='+driver+';PORT=1433;SERVER='+server+';PORT=1443;DATABASE='+database+';UID='+username+';PWD='+ password)
#}
db = mysql.connector.connect(user="aishdblogin@aishdbserver", password="Qwerty123", host="aishdbserver.mysql.database.azure.com", port=3306, database="apsdb")
cursor = db.cursor()
cursor1 = db.cursor()
cursor2 = db.cursor()
print (db)
print ("connection successful")
# #block_blob_service = BlockBlobService(account_name='aishlogs',
#                                       account_key='VnKALk8wpTyN+cgBLwdH6b6mZ/XDYbvCeg5UlBfrdSV37JsaoE+tgo+YQcI1myxdkqB2+wL1h76/BWBVxVsjpA==')
# # print(block_blob_service)
# # block_blob_service.set_container_acl('aishimgcontainer', public_access=PublicAccess.Container)
# # print ('Blob connected')

app = Flask(__name__)\

@app.route('/')
#for login
@app.route('/', methods=['POST', 'GET'])
@app.route('/login.html')
@app.route('/login', methods=['POST', 'GET'])
def login():
    if request.method == 'POST':
        uname = request.form['Username']
        # checking username from database
        sql = "select first_name, last_name from user where user_name = '" + uname + "'"
        #print (sql)
        cursor.execute(sql)
        #print(cursor.rowcount)
        results = cursor.fetchall()
        #if username exists in database then go to upload page
        if cursor.rowcount > 0:

            #print(results)
            for row in results:

                return render_template('uploadFiles.html', username=uname, fname=row[0])
        return render_template('login.html')
    else:
        return render_template('login.html')



#for registration
@app.route('/register', methods=['POST', 'GET'])
def register():
        # pip freeze > requirements.txt on local to automatically insert packages into requirements.txt


        #Input fields
        uname = request.form['Username']
        fname = request.form['Firstname']
        lname = request.form['Lastname']
        #print(uname)
        #print(lname)
        #sql = "select user_name from user where user_name='" + uname + "'"
        #print(sql)
        #cursor.execute(sql)
        #res=cursor.fetchall();
        #print(res)
        if uname == '' or fname == '' or lname == '':
            flash('Fields cannot be empty')
            return render_template('register.html')

        sql = "insert into user values ('" + uname + "','" + fname + "','" + lname + "')"
        #print(sql)
        cursor.execute(sql)
        #res = cursor.fetchall()
        #print(res)
        db.commit()
        #db.close()
        return '<h1>Successful User Registration </h1><br><form action="../"><input type="Submit" value="Login Now"></form>'

@app.route('/registerPage', methods=['POST', 'GET'])
def registerPage():
    return render_template('register.html')

# logout
@app.route('/logout', methods=['POST', 'GET'])
def logout():
    flash('You have been successfully logged out')
    return redirect(url_for('login'))

@app.route('/upload', methods=['POST'])
def upload():
    requestfile = request.files['file']
    file_name = requestfile.filename
    data = requestfile.read()
    #Connect_S3.Bucket('saipriya').put_object(Key=file_name, Body=data)
    return "File uploaded succesfully!"

@app.route('/csvupload', methods=['POST'])
def csvupload():
    #file_name = request.form['csvfile']
    #splitfile = file_name.split('.')[0]
    # for object in Connect_S3.Bucket('saipriya').objects.all():
    #print(object.key)
    # if 'boat.csv' == object.key:
    #     body = object.get()['Body'].read()
    #     mystr = []
    #     str = body.split('\n')[0]
    #     print(str)
    #     mystr = str.split(',')
        # cursor = myConnection.cursor()
    # droptable="DROP TABLE IF EXISTS %s"%splitfile
    # print( droptable)
    # cursor.execute(droptable)
    # print ("Table dropped successfully")
    # print(mystr[0], len(mystr))
    # executequery1="create table quakes (time text,latitude double,longitude double,depth double,mag double,magType text,nst text,gap text,dmin text,rms double,net text,id text,updated text,place text,type text,horizontalError double,depthError double,magError text,magNst text,status text,locationSource text,magSource text)"
    # cursor.execute(executequery1)
    executequery2 = 'load data local infile \'C:/Users/aishw/Downloads/Cloud computing/Assignment3/data/Education.csv \' into table Education fields terminated by \',\' optionally enclosed by \'"\' lines terminated by \'\n\' ignore 1 lines;'

    executequery3 = 'load data local infile \'C:/Users/aishw/Downloads/Cloud computing/Assignment3/data/quakes.csv \' into table quakes fields terminated by \',\' optionally enclosed by \'"\' lines terminated by \'\n\' ignore 1 lines;'

    executequery4 = 'load data local infile \'C:/Users/aishw/Downloads/Cloud computing/Assignment3/data/USZipcodes.csv \' into table USZipcodes fields terminated by \',\' optionally enclosed by \'"\' lines terminated by \'\n\' ignore 1 lines;'

    executequery5 = 'load data local infile \'C:/Users/aishw/Downloads/Cloud computing/Assignment3/data/Starbucks.csv \' into table Starbucks fields terminated by \',\' optionally enclosed by \'"\' lines terminated by \'\n\' ignore 1 lines;'

    #cursor.execute(executequery2)
    #cursor.execute(executequery3)
    cursor.execute(executequery4)
    #cursor.execute(executequery5)

    #count="select count(*) from quakes"
    #sql = "select count(*) from Education"
    #sql = "select count(*) from Starbucks"
    sql = "select count(*) from USZipcodes"
    cursor.execute(sql)
    result = cursor.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print (str(c) + ':' + str(res))
        str1 += str(c) + ':' + str(res) + '<br>'

    db.commit()
    result=result #str(res)
    return render_template('uploadFiles.html', rdscount=result)

@app.route('/sqlexecute', methods=['POST'])
def sqlexecute():
    limit = request.form['limit']
    starttime = time.time()
    print(starttime)
    cursor.execute(query + limit)
    endtime = time.time()
    print('endtime')
    totalsqltime = endtime - starttime
    print(totalsqltime)
    return render_template('uploadFiles.html', rdstime1=totalsqltime)

@app.route('/cleanexecute',methods=['POST'])
def cleanexecute():
    save="savepoint s1"
    cursor.execute(save)
    print ("save point created")
    safeupdate="SET SQL_SAFE_UPDATES = 0"
    cursor.execute(safeupdate)
    cleanquery="update quakes set depth=3.6 where mag=2.8"
    cursor.execute(cleanquery)
    print ("executed query")
    s="select * from quakes where depth=3.6"
    cursor.execute(s)
    result =  cursor.fetchall()
    c = 0
    str1 = " "
    for row in result:
        c = c + 1
        print (str(c) + ':' + str(row))
        str1 += str(c) + ':' + str(row) + '<br><br>'
    db.commit()
    return 'Executed'

@app.route('/query1', methods=['POST'])
def query1():
    q1="select * from quakes where mag between (select min(mag) from quakes) and (select max(mag) from quakes) having place like '%Alaska'";
    cursor.execute(q1)
    result =  cursor.fetchall()
    c = 0
    str1 = " "
    for row in result:
        c = c + 1
        print (str(c) + ':' + str(row))
        str1 += str(c) + ':' + str(row) + '<br><br>'
    return str(str1)

@app.route('/query2', methods=['POST'])
def query2():
    r1=request.form['val1']
    r2=request.form['val2']
    q2="select * from quakes where place like '%"+r1+"' or place like '%"+r2+"'"
    cursor.execute(q2)
    result =  cursor.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print (str(c) + ':' + str(res))
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str(str1)

@app.route('/query3', methods=['POST'])
def query3():
    r1 = request.form['val1']
    print( r1)
    r2 = request.form['val2']
    q2="select * from quakes where DAY(time) between day('%s') and day('%s')"%(r1,r2)
    cursor.execute(q2)
    result =  cursor.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print (str(c) + ':' + str(res))
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str1

@app.route('/query4', methods=['POST'])
def query4():
    r1 = request.form['val1']
    print( r1)
    r2 = request.form['val2']
    r3 = request.form['val3']
    # q2 = "select * from quakes where mag between %s and %s"%(r1,r2)
    q2 = "select * from quakes where DAY(time) between day('%s') and day('%s')" % (r1, r2)
    cursor.execute(q2)
    result =  cursor.fetchall()
    c = 0
    str1 = " "
    for res in result:
        c = c + 1
        print (str(c) + ':' + str(res))
        str1 += str(c) + ':' + str(res) + '<br><br>'
    return str1

# @app.route('/memexecute', methods=['POST'])
# def memexecute():
#     limit = request.form['limit']
#     cursor.execute(query + limit)
#     result =  cursor.fetchall()
#     memcache.set(hash, result)
#     c = 0
#     for res in result:
#         c = c + 1
#         print(str(c) + ':' + str(res))
#     starttime = time.time()
#     memresult = memcache.get(hash)
#     endtime = time.time()
#     total = endtime - starttime
#     print('Time taken by memcache ', total)
#     return render_template('uploadFiles.html', rdstime2=total)


























if __name__ == '__main__':
    app.debug = True
    app.run(port=5004)


