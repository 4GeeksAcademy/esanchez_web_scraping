import sqlite3
import requests
import pandas as pd

url = 'https://en.wikipedia.org/wiki/List_of_most-streamed_songs_on_Spotify'
response = requests.get(url)
tables = pd.read_html(url)
print(f'Total tables: {len(tables)}')
df = tables[0]
#PASO 4
df.dropna(inplace=True)
for col in df.columns:
    df[col] = df[col].astype(str).str.replace('$','').str.replace('B', '')
#PASO 5
conexion = sqlite3.connect('top_songs.db')
try:
    conexion.execute("""
        CREATE TABLE IF NOT EXISTS most_streamed_songs(
            rank INTEGER PRIMARY KEY AUTOINCREMENT, 
            song VARCHAR(255) NOT NULL, 
            artist VARCHAR(255) NOT NULL,
            streams_billions INT NOT NULL,
            release_date DATETIME NOT NULL, 
            ref INT
        )
    """)
    print("Se ha creado la tabla most_streamed_songs")
except sqlite3.OperationalError:
    print("la tabla most_streamed_songs ya existe")
finally:
    conexion.close()

conexion = sqlite3.connect("top_songs.db")
conexion.execute("INSERT INTO most_streamed_songs(song, artist, streams_billions, release_date, ref) VALUES (?, ?, ?, ?, ?)", ("Hablando en plata", "Melendi", 20354845, "2003-05-03", 2))
conexion.execute("INSERT INTO most_streamed_songs(song, artist, streams_billions, release_date, ref) VALUES (?, ?, ?, ?, ?)", ("Quédate", "Quevedo", 369258147, "2022-11-11", 3))
conexion.execute("INSERT INTO most_streamed_songs(song, artist, streams_billions, release_date, ref) VALUES (?, ?, ?, ?, ?)", ("Rosas", "La Oreja de Van Gogh", 2365478, "2003-09-26", 4))
conexion.commit()
conexion.close()
#PASO 6
conexion = sqlite3.connect("top_songs.db")
cursor = conexion.execute("SELECT rank, song, artist, streams_billions, release_date, ref FROM most_streamed_songs")
for fila in cursor:
    print(fila)
conexion.close()