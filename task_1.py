import sqlite3
import json

from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk


def extract():   # Лучше дать более развёрнутое имя функции, так будет понятнее что она делает.(В данном коде не обязательно, но если в проекте будет много строк кода и много функций, то смысл функции из ее названия будет сложно понять)
    """
    extract data from sql-db
    :return:
    """
    connection = sqlite3.connect("db.sqlite")  # Советую делать проверку на ошибку, вдруг не выйдет подключится к БД, что тогда произойдёт?
    cursor = connection.cursor()

    # Наверняка это пилится в один sql - запрос, но мне как-то лениво)  # Неусемстный комментарий, комментарии должны помогать человеку понять смысл кода, а не сообщять ему о вашем состоянии), лучше воздержаться от подобных комментариев!.

    # Получаем все поля для индекса, кроме списка актеров и сценаристов, для них только id  # Хороший комментарий, но следует в конце поставить точку, комментарий это законченное предложение(можно не ставить точку только в коротких коментариях).
    cursor.execute("""
        select id, imdb_rating, genre, title, plot, director,
        -- comma-separated actor_id's
        (
            select GROUP_CONCAT(actor_id) from
            (
                select actor_id
                from movie_actors
                where movie_id = movies.id
            )
        ),
        
        max(writer, writers)
        from movies
    """)

    raw_data = cursor.fetchall()  # Соединение с БД не закрывается, обязательно нужно закрывать его, после окончания работы с БД

    # cursor.execute('pragma table_info(movies)')    # Неуместный комментарий, если строчки кода не нужны после тестирования лучше избавиться от них, что бы не смущать человека читающего код.
    # pprint(cursor.fetchall())

    # Нужны для соответсвия идентификатора и человекочитаемого названия  #Опять же, лучше в конце поставить точку.
    actors = {row[0]: row[1] for row in cursor.execute('select * from actors where name != "N/A"')}
    writers = {row[0]: row[1] for row in cursor.execute('select * from writers where name != "N/A"')}

    return actors, writers, raw_data


def transform(__actors, __writers, __raw_data):  # Лучше дать более развёрнутое имя функции, так будет понятнее что она делает.(В данном коде не обязательно, но если в проекте будет много строк кода и много функций, то смысл функции из ее названия будет сложно понять)
    """
                        
    :param __actors:
    :param __writers:
    :param __raw_data:
    :return:             # Лучше будет оставить ещё 1 пустую строку перед ковычками.
    """
    documents_list = []
    for movie_info in __raw_data:
        # Разыменование списка   # Лучше разыменовени вынести в отдельную функцию, если будет много строчек кода и функций, проще будет править.
        movie_id, imdb_rating, genre, title, description, director, raw_actors, raw_writers = movie_info

        if raw_writers[0] == '[':
            parsed = json.loads(raw_writers)
            new_writers = ','.join([writer_row['id'] for writer_row in parsed])
        else:
            new_writers = raw_writers

        writers_list = [(writer_id, __writers.get(writer_id)) for writer_id in new_writers.split(',')]
        actors_list = [(actor_id, __actors.get(int(actor_id))) for actor_id in raw_actors.split(',')]

        document = {
            "_index": "movies",
            "_id": movie_id,
            "id": movie_id,
            "imdb_rating": imdb_rating,
            "genre": genre.split(', '),
            "title": title,
            "description": description,
            "director": director,
            "actors": [
                {
                    "id": actor[0],
                    "name": actor[1]
                }
                for actor in set(actors_list) if actor[1]
            ],
            "writers": [
                {
                    "id": writer[0],
                    "name": writer[1]
                }
                for writer in set(writers_list) if writer[1]
            ]
        }

        for key in document.keys():
            if document[key] == 'N/A':
                # print('hehe')  # # Неуместный комментарий, если строчки кода не нужны после тестирования лучше избавиться от них, что бы не смущать человека читающего код.
                document[key] = None

        document['actors_names'] = ", ".join([actor["name"] for actor in document['actors'] if actor]) or None
        document['writers_names'] = ", ".join([writer["name"] for writer in document['writers'] if writer]) or None

        import pprint  #  Все подключения пишутся в самом начале кода!
        pprint.pprint(document)

        documents_list.append(document)

    return documents_list

def load(acts):  # Лучше дать более развёрнутое имя функции, так будет понятнее что она делает.(В данном коде не обязательно, но если в проекте будет много строк кода и много функций, то смысл функции из ее названия будет сложно понять)
    """

    :param acts:
    :return:  
    """         # Лучше будет оставить ещё 1 пустую строку перед ковычками.
    es = Elasticsearch([{'host': '192.168.1.252', 'port': 9200}])
    bulk(es, acts)

    return True

if __name__ == '__main__':
    load(transform(*extract()))
