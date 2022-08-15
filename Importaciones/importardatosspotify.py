import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

# Se carga a una data el archivo csv delimitdo por comas y se hace una omisió del titulo 
# tcon skiprows
empdata = pd.read_csv('Spotify-2022.csv', 
	index_col=False, delimiter = ',',header=None, skiprows=1)
empdata.head()

try:
    # Se intenta la conexión al servidor de base de datos
    conn = msql.connect(host='localhost', database='music-bdi', 
        user='ghost', password='123')
    # Si hay conexion crea un corsor para hacer acciones de sql
    if conn.is_connected():
        cursor = conn.cursor()
        # Seleccionar la base de datos
        cursor.execute("select database();")
        record = cursor.fetchone()
        # Te informa que esta conectado a l base de datos específica
        print("Estas conectado a la base de datos: ", record)
        #loop Encargado para leer iterar los datos de acuerdo a las filas y columnas
        for i,row in empdata.iterrows():
            #El %S significa que tomará los strings de cada valor
            sql = "INSERT INTO spotify VALUES (%s,%s,%s,%s,%s,%s)"
            cursor.execute(sql, tuple(row))
            conn.commit()
        print("Datos Insertados del 2022")
except Error as e:
            print("Error al conectar con MySQL o la base de datos", e)
