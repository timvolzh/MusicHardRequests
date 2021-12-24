import psycopg2
import sqlalchemy
from pprint import pprint
import csv


# Подключение БД
db = 'postgresql://postgres:1111@localhost:5432/postgres'
engine = sqlalchemy.create_engine(db)
connection = engine.connect()


'''Получение и запись в файл результатов запросов'''

# res1 - количество исполнителей в каждом жанре
res1 = connection.execute(
    "SELECT genre_name , count(singer) FROM genre g LEFT JOIN singergenre ON g.genre_id=singergenre.genre GROUP BY g.genre_name;").fetchmany(10)
with open('res1.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res1)

# res2 - количество треков, вошедших в альбомы 2019-2020 годов
res2 = connection.execute(
    f"SELECT album_name, count(song) FROM album LEFT JOIN song ON album.album_id=song.album WHERE album.release_year BETWEEN 2019 AND 2020 GROUP BY album.album_name;").fetchmany(10)
with open('res2.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res2)

# res3 - средняя продолжительность треков по каждому альбому
res3 = connection.execute(
    "SELECT album_name, AVG(duration) FROM album LEFT JOIN song ON album.album_id=song.album GROUP BY album.album_name ").fetchmany(10)
with open('res3.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res3)

# res4 - все исполнители, которые не выпустили альбомы в 2020 году
res4 = connection.execute(
    "SELECT singer_name FROM singer LEFT JOIN albumsinger ON singer.singer_id = albumsinger.singer LEFT JOIN album ON albumsinger.album = album.album_id  WHERE album.release_year != 2020;").fetchmany(10)
with open('res4.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res4)

# res5 - названия сборников, в которых присутствует конкретный исполнитель (Niletto)
res5 = connection.execute(
    "SELECT collection_name FROM collection LEFT JOIN collectionsong ON collection.id = collectionsong.collection LEFT JOIN song ON collectionsong.song = song.id LEFT JOIN album on song.album = album.album_id LEFT JOIN  albumsinger  ON album.album_id = albumsinger.album LEFT JOIN singer ON albumsinger.singer = singer.singer_id WHERE singer_name = 'Niletto';").fetchmany(10)
with open('res5.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res5)

# res6 - название альбомов, в которых присутствуют исполнители более 1 жанра
res6 = connection.execute(
    "SELECT album_name FROM album left join albumsinger ON album.album_id = albumsinger.album left join singer ON albumsinger.singer = singer.singer_id WHERE singer_name = (select singer_name from singer LEFT JOIN singergenre ON singer.singer_id = singergenre.singer GROUP BY singer_name having count(genre) > 1) GROUP BY album_name ;").fetchmany(10)
with open('res6.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res6)

# res7 - наименование треков, которые не входят в сборники
res7 = connection.execute(
    "SELECT song_name, collectionsong.collection FROM song LEFT JOIN collectionsong ON song.id = collectionsong.song WHERE collectionsong.collection IS NULL;").fetchmany(10)
with open('res7.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res7)

# res8 -исполнитель, написавший самый короткий по продолжительности трек
res8 = connection.execute(
    "SELECT singer_name FROM singer LEFT JOIN albumsinger ON singer.singer_id = albumsinger.singer LEFT JOIN album ON albumsinger.album = album.album_id LEFT JOIN song ON album.album_id = song.album WHERE duration = (SELECT min(duration) FROM song) GROUP BY singer_name ").fetchmany(10)
with open('res8.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res8)

# res9 - название альбомов, содержащих наименьшее количество треков
res9 = connection.execute(
    "SELECT album_name FROM(SELECT album_name, count (song.song_name) AS SongCount FROM album LEFT JOIN song ON album.album_id = song.album GROUP BY album_name) AS Counter WHERE SongCount = (SELECT min(SongCount) FROM (select album_name AS AlbumName, count (song.song_name) AS SongCount FROM album LEFT JOIN song ON album.album_id = song.album GROUP BY album_name) AS minCount);").fetchmany(10)
with open('res9.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res9)