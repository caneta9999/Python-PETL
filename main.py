#Credits and Thanks
#https://www.kaggle.com/datasets/knightbearr/sales-product-data
#https://www.bankofcanada.ca/valet/docs
#https://www.youtube.com/watch?v=InLgSUw_ZOE
#https://stackoverflow.com/questions/19472922/reading-external-sql-script-in-python
#https://www.datacamp.com/tutorial/mysql-python
#https://stackoverflow.com/questions/43356116/petl-fails-to-load-data-to-mysql-db

import os
import sys
import petl
import configparser
import requests
from datetime import datetime
from decimal import Decimal
import mysql.connector as mysql
import json

def createSQL(filename, connection):
    file = open(filename,'r')
    sqlCommands = file.read().split(';')
    file.close()
    cursor = connection.cursor()
    for command in sqlCommands:
        if command.strip() == "":
            continue
        try:
            cursor.execute(command)
        except Exception as e:
            print("The command was ignored: ", str(e))

def endProgram(connection=True):
    sys.exit()
    if connection:
        dbConnection.close()

config = configparser.ConfigParser()
try:
    config.read('config.ini')
except Exception as e:
    print('couldn\'t read configuration file:' + str(e))
    endProgram(False)

startDate = config['CONFIG']['startDate']
endDate = config['CONFIG']['endDate']
url = config['CONFIG']['url']
user = config['CONFIG']['user']
password = config['CONFIG']['password']
database = config['CONFIG']['database']

dbConnection = ''
try:
    dbConnection = mysql.connect(user=user,passwd=password,db=database)
except Exception as e:
    print('couldn\'t connect to database:' + str(e))
    endProgram(False)

createSQL('create.sql', dbConnection)

months = ['January', 'February', 'March', 'April', 'May', 'June', 'July', 'August', 'September', 'October', 'November', 'December']
years = [2019]
sales = [petl.fromcsv(f'Sales_{month}_{year}.csv') for year in years for month in months]
salesGrouped = sales[0]
sales.pop(0)
for sale in sales:
    salesGrouped = petl.stack(salesGrouped,sales)
salesGrouped = petl.cutout(salesGrouped,'Purchase Address')
salesGrouped = petl.cutout(salesGrouped,'Order ID')
headers = {'Product': 'product', 'Quantity Ordered': 'quantity', 'Price Each': 'price', 'Order Date': 'date'}
salesGrouped = petl.rename(salesGrouped, headers)
salesGrouped = petl.selectisnot(salesGrouped,'product', '')
salesGrouped = petl.convert(salesGrouped,'date', lambda d: datetime.strptime(d[:8],'%m/%d/%y'))
salesGrouped = petl.convert(salesGrouped,'quantity', lambda q: Decimal(q))
salesGrouped = petl.convert(salesGrouped,'price', lambda p: Decimal(p))
#print(petl.look(salesGrouped))
#print(salesGrouped)
print(petl.head(salesGrouped, 5))

BOCResponse = []
exchanges = ''
try:
    BOCResponse = requests.get(url+f'?start_date={startDate}&end_date={endDate}')
except Exception as e:
    print('couldn\'t make request:' + str(e))
    endProgram()
    
if (BOCResponse.status_code == 200):
    BOCRaw = json.loads(BOCResponse.text)
    BOCDates = []
    BOCRates = []
    for row in BOCRaw['observations']:
        BOCDates.append(datetime.strptime(row['d'],'%Y-%m-%d'))
        BOCRates.append(Decimal(row['FXUSDCAD']['v']))
    exchanges = petl.fromcolumns([BOCDates,BOCRates],header=['date','rate'])
    print(petl.head(exchanges, 5))
else:
    print('no response...')
    endProgram()

#salesGrouped = petl.lookupjoin(salesGrouped,exchanges,key='date')
#salesGrouped = petl.filldown(salesGrouped,'rate')
salesGrouped = petl.join(salesGrouped,exchanges,key='date')
salesGrouped = petl.addfield(salesGrouped, 'total', lambda rec: rec.quantity * rec.price)
salesGrouped = petl.addfield(salesGrouped, 'CAD_conversion', lambda rec: rec.total * rec.rate)
salesGrouped = petl.cutout(salesGrouped, 'rate')
print(petl.head(salesGrouped, 5))

try:
    pass
    #These commands cause the error: Parameters for query must be list or tuple.
    #I think they are correct, I tried many things and searched a lot but I haven't found a way to solve this problem.
    '''
    petl.io.todb(salesGrouped,dbConnection, 'sales')
    petl.io.todb(exchanges,dbConnection,'exchanges')
    '''
except Exception as e:
    print('couldn\'t write to database: ' + str(e))
finally:
    endProgram()
