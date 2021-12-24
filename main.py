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
res1 = connection.execute("SELECT genre_name , count(singer) FROM genre g LEFT JOIN singergenre ON g.genre_id=singergenre.genre GROUP BY g.genre_name;").fetchmany(10)
with open('res1.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res1)

# res2 - количество треков, вошедших в альбомы 2019-2020 годов
res2 = connection.execute(
    f"select album_name, count(song) from album left join song on album.album_id=song.album where album.release_year between 2019 and 2020 group by album.album_name;").fetchmany(10)
with open('res2.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res2)

# res3 - средняя продолжительность треков по каждому альбому
res3 = connection.execute("select album_name, AVG(duration) from album left join song on album.album_id=song.album group by album.album_name ").fetchmany(10)
with open('res3.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res3)

# res4 - все исполнители, которые не выпустили альбомы в 2020 году
res4 = connection.execute("select singer_name from singer left join albumsinger on singer.singer_id = albumsinger.singer left join album on albumsinger.album = album.album_id  where album.release_year != 2020;").fetchmany(10)
with open('res4.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res4)

# res5 - названия сборников, в которых присутствует конкретный исполнитель (Niletto)
res5 = connection.execute("select collection_name from collection left join collectionsong on collection.id = collectionsong.collection left join song on collectionsong.song = song.id left join album on song.album = album.album_id left join  albumsinger  on album.album_id = albumsinger.album left join singer on albumsinger.singer = singer.singer_id where singer_name = 'Niletto';").fetchmany(10)
with open('res5.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res5)

# res6 - название альбомов, в которых присутствуют исполнители более 1 жанра
res6 = connection.execute(
    "select album_name from album left join albumsinger on album.album_id = albumsinger.album left join singer on albumsinger.singer = singer.singer_id where singer_name = (select singer_name from singer left join singergenre on singer.singer_id = singergenre.singer group by singer_name having count(genre) > 1) group by album_name ;").fetchmany(10)
with open('res6.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res6)

# res7 - наименование треков, которые не входят в сборники
res7 = connection.execute(
    "select song_name, collectionsong.collection from song left join collectionsong on song.id = collectionsong.song where collectionsong.collection is null;").fetchmany(10)
with open('res7.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res7)

# res8 -исполнитель, написавший самый короткий по продолжительности трек
res8 = connection.execute(
    "select singer_name from singer left join albumsinger on singer.singer_id = albumsinger.singer left join album on albumsinger.album = album.album_id left join song on album.album_id = song.album where duration = (select min(duration) from song) group by singer_name ").fetchmany(10)
with open('res8.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res8)

# res9 - название альбомов, содержащих наименьшее количество треков
res9 = connection.execute(
    "select album_name from(select album_name, count (song.song_name) as SongCount from album left join song on album.album_id = song.album group by album_name) as Counter where SongCount = (select min(SongCount) from (select album_name as AlbumName, count (song.song_name) as SongCount from album left join song on album.album_id = song.album group by album_name) as minCount);").fetchmany(10)
with open('res9.csv', 'w', newline='', encoding='utf-8') as f:
    writer = csv.writer(f)
    writer.writerows(res9)