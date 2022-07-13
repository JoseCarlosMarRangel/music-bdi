import mysql.connector as msql
from mysql.connector import Error
import pandas as pd

def menu():
    bienvenida = """
    ### Bienvenido a MUSIC-BI ###
    La base de datos seleccionada es music-bdi \n
    """
    menu = ('1.- ¿Cuál es la disquera con más canciones entre el 2020 y 2022?\n'
            '     2.- ¿Cuál es la canción con más reproducciones en spotify entre el 2020 y 2022?\n'
            '     3.- ¿Cuál es la canción con más reproducciones en youtube entre el 2020 y 2022?\n'
            '     4.- ¿Cuál fue el artista más popular entre los tiempos 2020 y 2022 en youtube?\n'
            '     5.- ¿Cuál fue el artista más popular entre los tiempos 2020 y 2022 en spotify? \n'
            '     6.- Salir'
        )

    print(bienvenida,menu)


def disquera_canciones(conn):
    sql = "SELECT disquera, COUNT(disquera) AS canciones FROM  spotify WHERE ano BETWEEN '2020' AND '2022' GROUP BY disquera ORDER BY canciones DESC"
    resultados_df = pd.read_sql_query(sql,conn)
    resultados_df.to_csv("disquera_con_más_canciones.csv")
    print("CSV GENERADO")

def cancion_reproducciones_spotify(conn):
    sql = "SELECT nom_cancion, reproducciones FROM spotify WHERE ano BETWEEN '2020' AND '2022' ORDER BY reproducciones DESC;"
    resultados_df = pd.read_sql_query(sql,conn)
    resultados_df.to_csv("cancion_reproducciones_spotify.csv")
    print("CSV GENERADO")

def cancion_reproducciones_youtube(conn):
    sql = "SELECT nom_cancion, reproducciones FROM youtube WHERE ano BETWEEN '2020' AND '2022' ORDER BY reproducciones DESC;"
    resultados_df = pd.read_sql_query(sql,conn)
    resultados_df.to_csv("cancion_reproducciones_youtube.csv")
    print("CSV GENERADO")

def artista_popular_youtube(conn):
    sql = "SELECT nom_artista, reproducciones FROM youtube WHERE ano BETWEEN '2020' AND '2022' ORDER BY reproducciones DESC;"
    resultados_df = pd.read_sql_query(sql,conn)
    resultados_df.to_csv("artista_popular_youtube.csv")
    print("CSV GENERADO")

def artista_popular_spotify(conn):
    sql = "SELECT nom_artista, reproducciones FROM spotify WHERE ano BETWEEN '2020' AND '2022' ORDER BY reproducciones DESC;"
    resultados_df = pd.read_sql_query(sql,conn)
    resultados_df.to_csv("artista_popular_spotify.csv")
    print("CSV GENERADO")

def conectar():
    try:
    # Se intenta la conexión al servidor de base de datos
        conn = msql.connect(host='localhost', database='music-bdi', user='paralax', password='123')
        if conn.is_connected():
            # si hay conexión se imprime el menu
            menu()

            while True:
                choice = input('¿Qué te gustaría saber?: ').lower()
                if choice == '1':
                    disquera_canciones(conn)
                elif choice == '2':
                    cancion_reproducciones_spotify(conn)
                elif choice == '3':
                    cancion_reproducciones_youtube(conn)
                elif choice == '4':
                    artista_popular_youtube(conn)
                elif choice == '5':
                    artista_popular_spotify(conn)
                elif choice == '6':
                    return
                else:
                    print(f'opción no correcta seleccionada: <{choice}>,intentalo de nuevo')
            
    except Error as e:
            print("Error al conectar con MySQL o la base de datos", e)





if __name__ == '__main__':
    conectar()