import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

# Se carga a una data el archivo csv delimitdo por comas y se hace una omisió del titulo 
# tcon skiprows
empdata = pd.read_csv('regional-mx-weekly-2022-06-16.csv', 
	index_col=False, delimiter = ',',header=None, skiprows=1)
empdata.head()

try:
    # Se intenta la conexión al servidor de base de datos
    conn = msql.connect(host='localhost', database='pruebacsv', user='paralax', password='123')
    # Si hay conexion crea un corsor para hacer acciones de sql
    if conn.is_connected():
        cursor = conn.cursor()
        # Seleccionar la base de datos
        cursor.execute("select database();")
        record = cursor.fetchone()
        # Te informa que esta conectado a l base de datos específica
        print("Estas conectado a la base de datos: ", record)
        #loop through the data frame
        for i,row in empdata.iterrows():
            #here %S means string values 
            sql = "INSERT INTO spotify VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("Datos Insertados")
except Error as e:
            print("Error al conectar con MySQL o la base de datos", e)
