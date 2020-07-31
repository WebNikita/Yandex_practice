from flask import Flask, abort, request, jsonify
import elasticsearch as ES  # В даном случае реккомендовал бы абсолютное импортирование, так как более читаемо, хотя так то-же приемлемо.

from validate import validate_args
                                      # По pep8 следует оставлять 2 строки после обьявления модулей и библиотек

app = Flask(__name__)


@app.route('/')
def index():
    return 'worked'

@app.route('/api/movies/')
def movie_list():
    validate = validate_args(request.args)# Не стоит называть переменные именем таким же как и импортируемый модуль, может возникнуть путаница

    if not validate['success']:
        return abort(422)

    # Проблема с неймингом, несовсем понятно что значит данная переменная(по умолчания чего?), следует дополнить имя переменной(пример: defaults_settings)
    defaults = {  
        'limit': 50,
        'page': 1,
        'sort': 'id',
        'sort_order': 'asc'
    }

    # Тут уже валидно все   # Комментарий непонятный, следует его расширить(мне как человеку не понятно что значит - "Тут уже все валидно". 
                            # комментарий нужен для более подробного разьяснения кода, в данном случаем он либо не нужен, либо следует его дополнить).
    for param in request.args.keys():
        defaults[param] = request.args.get(param)

    # Уходит в тело запроса. Если запрос не пустой - мультисерч, если пустой - выдает все фильмы  #Опять же, не полняй коментарий(кто уходит в тело?). По програме достаточно понятно, что происходит, я бы посоветовал воздержаться от ненужных коментариев
    body = {
        "query": {
            "multi_match": {
                "query": defaults['search'],
                "fields": ["title"]
            }
        }
    } if defaults.get('search', False) else {}  # Лучше перенести if на следующюю строку, так как if никак не относится к словарю body, и код так будет читаться лучше!

    body['_source'] = dict()
    body['_source']['include'] = ['id', 'title', 'imdb_rating']

    params = {
        # '_source': ['id', 'title', 'imdb_rating'],   # Неуместный комментарий, комментарии должны являться законченными предложениями. А это закоменчанная строчка кода, если она не нужна, ее следует удалить!
        'from': int(defaults['limit']) * (int(defaults['page']) - 1),
        'size': defaults['limit'],
        'sort': [
            {
                defaults["sort"]: defaults["sort_order"]
            }
        ]
    }

    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], ) # Переменная es_client обьявляется в 2х функциях, желательно вынести ее за функции и передавать как аргумент
    search_res = es_client.search(
        body=body,
        index='movies',
        params=params,
        filter_path=['hits.hits._source']
    )
    es_client.close()

    return jsonify([doc['_source'] for doc in search_res['hits']['hits']])


@app.route('/api/movies/<string:movie_id>')
def get_movie(movie_id):
    es_client = ES.Elasticsearch([{'host': '192.168.11.128', 'port': 9200}], )

    if not es_client.ping():
        print('oh(')  #  Лучше вывести сообщение об ошибке([ERROR] )

    search_result = es_client.get(index='movies', id=movie_id, ignore=404)

    es_client.close()

    if search_result['found']:
        return jsonify(search_result['_source'])

    return abort(404)

if __name__ == "__main__":
    app.run(host='0.0.0.0', port=80)
