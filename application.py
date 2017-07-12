import os
import csv
import mysql.connector
import memcache
from flask import Flask, request, render_template, redirect, url_for
import datetime
import hashlib
import random

app = Flask(__name__)

AWS_ACCESS_KEY_ID = ""
AWS_SECRET_KEY = ""

# conns3 = boto.connect_s3(AWS_ACCESS_KEY_ID, AWS_SECRET_KEY)

# memcache
mem = memcache.Client([''], debug=0)
print 'memcache done'

# executed only once!
bucket_name = "userenteredbucketname"


# bucket = conns3.create_bucket(bucket_name)

def initdbConn():
    return mysql.connector.connect(host="", user="",
                                   passwd="", db="")


@app.route('/')
def init_aws():
    return render_template("index.html")


@app.route('/todb')
def todb():
    filename = ''
    file = open(filename)
    # break
    csv_file = csv.reader(file)
    file_str = "( "
    for row in csv_file:
        for i in row:
            file_str += (i + ' VARCHAR(50), ')
        break
    file_str += ' ID_POP INT AUTO_INCREMENT, PRIMARY KEY (ID_POP))'
    conn = initdbConn()
    c = conn.cursor()
    queryStr = 'Create Table university' + ' ' + file_str
    print (queryStr)
    c.execute(queryStr)
    conn.commit()
    t1 = datetime.datetime.now()
    queryString = "LOAD DATA LOCAL INFILE '" + filename + "' INTO TABLE university FIELDS TERMINATED BY ',' ENCLOSED BY '\"' LINES TERMINATED BY '\r\n' IGNORE 1 LINES"
    print (queryString)
    c.execute(queryString)
    conn.commit()
    t2 = datetime.datetime.now()
    # queryString = "SELECT COUNT(IPEDSID) from university"
    # total = c.execute(queryString)
    # print total
    # conn.commit()
    return "<br><hr> <i> The time taken to insert file data in RDB is : " + str(t2 - t1) + "</i><hr>"


@app.route('/latlong', methods=['GET', 'POST'])
def nearby():
    conn = initdbConn()
    c = conn.cursor()
    if request.method == 'POST':
        lat = request.form['lat']
        long = request.form['long']
        query = "Select * from university where LATITUDE like '" + lat + "'and LONGITUDE like'" + long + "';"
        # lat_new1 = float(lat) + 2
        # long_new1 = float(long) +2
        # query2 = "Select * from university where LATITUDE like '" + str(lat_new1) + "'and LONGITUDE like'" + str(long_new1) + "';''"
        # lat_new2 = float(lat) - 2
        # long_new2 = float(long) - 2
        # query3 = "Select * from university where LATITUDE like '" + str(lat_new2) + "'and LONGITUDE like'" + str(long_new2) + "';''"
        t1 = datetime.datetime.now()
        result = ""
        c.execute(query)
        data = c.fetchall()
        for row in data:
            result += str(row) + "\n"
        # c.execute(query2)
        # conn.commit()
        # data = c.fetchall()
        # for row in data:
        #     result += str(row) + "\n"
        # c.execute(query3)
        # conn.commit()
        # data = c.fetchall()
        # for row in data:
        #     result += str(row) + "\n"
        conn.commit()
        t2 = datetime.datetime.now()

    return "<h1>Results</h1>" + "<hr> <i> The time taken to fire the queries is : " + str(
        t2 - t1) + "</i><hr>" + "<br>The result is : <br>" + result


@app.route('/total', methods=['GET', 'POST'])
def total():
    conn = initdbConn()
    c = conn.cursor(buffered=True)
    if request.method == 'POST':
        query = "Select * from university;"
        print query
        t1 = datetime.datetime.now()
        totalcount = c.execute(query)
        count = c.rowcount
        conn.commit()
        t2 = datetime.datetime.now()
    return "<br>The total rows are : <br>" + str(count) + "<hr> <i> The time taken for the queries is : " + str(t2 - t1)


@app.route('/city', methods=['GET', 'POST'])
def city():
    conn = initdbConn()
    c = conn.cursor(buffered=True)
    if request.method == 'POST':
        city = request.form['city']
        query = "Select GivenName, City, State from university where CITY  = '" + city + "' ;"
        q1hash = hashlib.sha256(query).hexdigest()
        t3 = datetime.datetime.now()
        mresult = mem.get(q1hash)
        t4 = datetime.datetime.now()
        t5 = t4 - t3
        mresults = []
        result = ""
        i = 0
        mtime = 0
        t1 = datetime.datetime.now()
        c.execute(query)
        mresult = mem.get(q1hash)
        if not mresult:
            data = c.fetchall()
            mem.set(q1hash, data)
            for row in data:
                result += str(row) + "\n"
        else:
            mtime = t5
            for row in mresult:
                i = i + 1
                mresults.append(str(i) + ':' + str(row))

    result = ""
    # data = c.fetchall()

    count = c.rowcount
    conn.commit()
    t2 = datetime.datetime.now()
    return "<h1>Results</h1>" + "<hr> <i> The time taken for the queries is : " + str(
        t2 - t1) + "</i><hr>" + "<br>The number of results are : <br>" + str(
        count) + "<br>The results are : <br>" + str(result) + "The memcache results are:" + str(mtime)


@app.route('/1000queries', methods=['GET', 'POST'])
def query1000():
    conn = initdbConn()
    c = conn.cursor(buffered=True)
    if request.method == 'POST':
        lat1 = request.form['latto']
        lat2 = request.form['latfrom']
        age = request.form['age']
        count = request.form['count']
        query = "Select * from university where latitude = '" + random.uniform(float(lat1), float(lat2)) + "' LIMIT 1"

        q1hash = hashlib.sha256(query).hexdigest()
        mst = datetime.datetime.now()
        mresult = mem.get(q1hash)
        met = datetime.datetime.now()
        mtt = met - mst
        mresults = []
        i = 0
        mtime = 0
        if mresult:
            mtime = mtt
            for row in mresult:
                i = i + 1
                mresults.append(str(i) + ':' + str(row))

        t1 = datetime.datetime.now()
        for x in range(1, int(count)):
            c.execute(query)
            mresult = mem.get(q1hash)
            if not mresult:
                result = c.fetchall()
                mem.set(q1hash, result)
        conn.commit()
        t2 = datetime.datetime.now()
        result = c.fetchall()
        mem.set(q1hash, result)

        return "<h1>Results</h1>" + "<hr> <i> The time taken to fire" + count + " queries is : " + str(
            t2 - t1) + "</i><hr>" + "<br>The memcache result is : <br>" + str(mtt)


@app.route('/5000queries', methods=['GET', 'POST'])
def query5000():
    conn = initdbConn()
    c = conn.cursor()
    if request.method == 'POST':
        state = request.form['state']
        query = "Select * from population where STATE = '" + state + "' LIMIT 1"
        t1 = datetime.datetime.now()
        print ("In if, before for")
        for x in range(1, 5000):
            c.execute(query)
        conn.commit()
        t2 = datetime.datetime.now()
        print ("In if, after for \n\n" + str(t2 - t1) + "\n\n")
        # result = ""
        # for em in resList:
        #    result += str(em)


port = os.getenv('PORT', '9000')
if __name__ == "__main__":
    app.debug = True
    app.run(host='0.0.0.0', port=int(port))


