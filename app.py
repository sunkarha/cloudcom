import codecs

import flask
from flask import Flask, render_template,request
import pyodbc
import csv


app = flask.Flask(__name__)
app.config["DEBUG"] = True


@app.route('/')
def login():
    return render_template("login.html")

@app.route('/dashboard')
def tableaudashboard():
    return render_template("tableau.html")

@app.route('/register')
def register():
    return render_template("register.html")

@app.route('/loginValidation',methods=['POST'])
def login_validation():
    error=None
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    cursor = conn.cursor()
    username=request.form['Uname']
    password=request.form['Pass']
    cursor.execute("""SELECT * FROM azurecloud.dbo.user_details where user_name = ? and password = ? """,[username,password])
    usersss=cursor.fetchone()
    conn.commit()
    if usersss != None :
        sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=10"
        cursor.execute(sql_select_query)
        data = cursor.fetchall()
        conn.close()
        return render_template("homepage.html", hshddata=data)
    else:
        error="Incorrect username or password"
        return render_template('login.html',error=error)

@app.route('/addUser',methods=['POST'])
def add_user():
    msg="Registered Sucessfully. Please login to continue"
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    c = conn.cursor()
    fnameu=request.form['Fname']
    lnameu=request.form['Lname']
    emailu=request.form['email']
    usernameu=request.form['username']
    pswu=request.form['Pass']
    c.execute("""SELECT * FROM azurecloud.dbo.user_details WHERE user_name=? """,(usernameu,))
    users=c.fetchone()
    if users==None:
        c.execute("""INSERT INTO azurecloud.dbo.user_details (first_name,last_name,email,user_name,password) VALUES (?,?,?,?,?)""",(fnameu,lnameu,emailu,usernameu,pswu))
        conn.commit()
        conn.close()
        msg="Registration Successful!"
        return render_template("login.html",msg=msg)
    else:
        error="Username already exists. Please try with different Username"
        conn.close()
        return render_template("register.html",error=error)


@app.route('/getrows', methods=['GET','POST'])
def getRows():
    hhnum=request.args.get('search')
   # hhnum=request.form['search']
    print(hhnum)
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    cursor = conn.cursor()

    #sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=?"
    cursor.execute("""select h.hshd_num, t.basket_num,t.product_num,t.purchase,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=?""",(hhnum,))
    data=cursor.fetchall()
    conn.close()
    return render_template("homepage.html",hshddata=data)

def decode_utf8(input_iterator):
    for l in input_iterator:
        yield l.decode('utf-8')

@app.route('/insertdata', methods=['GET', 'POST'])
def insertCSVData():
    tablename= None
    errmsg=None
    fileName = request.files['file']
    conn = pyodbc.connect(
        'DRIVER={SQL Server};SERVER=tcp:azurecloudassign.database.windows.net,1433;DATABASE=azurecloud;UID=azureadmin;PWD=Admin@123;')
    cursor = conn.cursor()
    reader = csv.reader(codecs.iterdecode(request.files['file'], 'utf-8'))
    next(reader)
    for row in reader:
        if 'products' in fileName.filename:
            tablename='PRODUCTS'
            cursor.execute('INSERT INTO azurecloud.dbo.products(PRODUCT_NUM, DEPARTMENT, COMMODITY,BRAND_TY,NATURAL_ORGANIC_FLAG) VALUES(?,?,?,?,?)',row)
        elif 'households' in fileName.filename:
            tablename='HOUSEHOLDS'
            cursor.execute('INSERT INTO azurecloud.dbo.households(HSHD_NUM,L,AGE_RANGE,MARITAL,INCOME_RANGE,HOMEOWNER,HSHD_COMPOSITION,HH_SIZE,CHILDREN) VALUES(?,?,?,?,?,?,?,?,?)',row)
        elif 'transactions' in fileName.filename:
            tablename='TRANSACTIONS'
            cursor.execute('INSERT INTO azurecloud.dbo.transactions(BASKET_NUM,HSHD_NUM,PURCHASE,PRODUCT_NUM,SPEND,UNITS,STORE_R,WEEK_NUM,YEAR) VALUES(?,?,?,?,?,?,?,?,?)',row)
        else:
            errmsg="Incorrect file provided. Allowed filenames households.csv or transactions.csv or products.csv"
    conn.commit()
    sql_select_query = "select h.hshd_num, t.basket_num,t.product_num,t.purchase,p.department,p.commodity,t.spend,t.units,t.store_r,t.week_num,t.year,h.L,h.age_range,h.marital,h.income_range,h.homeowner,h.hshd_composition,h.hh_size,h.children from households as h,transactions as t , products as p where h.hshd_num=t.hshd_num and t.product_num=p.product_num and h.hshd_num=10"
    cursor.execute(sql_select_query)
    data = cursor.fetchall()
    insert_msg="Data is inserted successfully in the table"
    conn.close()
    if errmsg==None:
        return render_template("homepage.html", hshddata=data,insertmsg=insert_msg,tablename=tablename)
    else:
        return render_template("homepage.html",hshddata=data,errmsg=errmsg)


    #return render_template('homepage.html')

app.run()