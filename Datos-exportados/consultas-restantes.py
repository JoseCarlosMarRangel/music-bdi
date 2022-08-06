import mysql.connector as msql
from mysql.connector import Error
import pandas as pd


def conectar():
    try:
        # Se intenta conectar al servidor
        conn = msql.connect(
            host='localhost', database='music-bdi', user='paralax', password='123')
        rest = cancion_reproducciones_spotify(conn)
        rest2 = cancion_reproducciones_youtube(conn)
        cancion_spotify_youtube(conn, rest)
        cancion_youtube_spotify(conn, rest2)
        print(rest)
        print(rest2)
        badbunny_youtube(conn)
        badbunny_spotify(conn)
        disquera_youtube(conn)
        disquera_spotify(conn)
    except Error as e:
        print("error de conexión")


# Canción mas escuchada de Spotify en el 2022
def cancion_reproducciones_spotify(conn):
    sql = "SELECT nom_cancion,max(reproducciones) FROM spotify WHERE ano = 2022;"
    resultados_df = pd.read_sql_query(sql, conn)
    resultado = resultados_df.iat[0, 0]
    return resultado


# Canción mas escuchada de Youtube en el 2022
def cancion_reproducciones_youtube(conn):
    sql = "SELECT nom_cancion,max(reproducciones) FROM youtube WHERE ano = 2022;"
    resultados_df = pd.read_sql_query(sql, conn)
    resultado = resultados_df.iat[0, 0]
    return resultado


# Cuantas reprodicciones tiene en Youtube la cancion mas escuchada de Spotify en el 2022
def cancion_spotify_youtube(conn, rest):
    sql = "SELECT nom_cancion, reproducciones FROM youtube WHERE nom_cancion = '" + rest + "';"
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("cancion_spotify_youtube.csv")
    print("csv creado")


# Cuantas reprodicciones tiene en Spotify la cancion mas escuchada de YoubTube en el 2022
def cancion_youtube_spotify(conn, rest2):
    sql = "SELECT nom_cancion, reproducciones FROM spotify WHERE nom_cancion = '" + rest2 + "';"
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("cancion_youtube_spotify.csv")
    print("csv creado")


# Cuantas repoducciones tiene BadBunny en Youtube desde el 2020 al 2022
def badbunny_youtube(conn):
    sql = """SELECT nom_artista,SUM(reproducciones) 
    FROM youtube WHERE nom_artista = 'Bad Bunny' AND ano BETWEEN '2020' AND '2022';"""
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("badbunny_youtube.csv")
    print("csv creado")


# Cuantas repoducciones tiene BadBunny en spotify desde el 2020 al 2022
def badbunny_spotify(conn):
    sql = """SELECT nom_artista,SUM(reproducciones) 
    FROM spotify WHERE nom_artista = 'Bad Bunny' AND ano BETWEEN '2020' AND '2022';"""
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("badbunny_spotify.csv")
    print("csv creado")


# Cual es la disquera con mas reprodicciones en 2020,2021 y 2022 en youtube?
def disquera_youtube(conn):
    sql = """SELECT disquera,SUM(reproducciones), ano 
    FROM youtube WHERE ano BETWEEN '2020' AND '2022' GROUP BY disquera ORDER BY SUM(reproducciones) DESC;"""
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("disquera_youtube.csv")
    print("csv creado")


# Cual es la disquera con mas reprodicciones en 2020,2021 y 2022 en spotify?
def disquera_spotify(conn):
    sql = """SELECT disquera,SUM(reproducciones), ano 
    FROM spotify WHERE ano BETWEEN '2020' AND '2022' GROUP BY disquera ORDER BY SUM(reproducciones) DESC;"""
    resultados_df = pd.read_sql_query(sql, conn)
    resultados_df.to_csv("disquera_spotify.csv")
    print("csv creado")


if __name__ == '__main__':
    conectar()
