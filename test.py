import sqlalchemy
import csv
from sqlalchemy import Table, Column, Float, String, Integer, Date, MetaData, ForeignKey, create_engine


def create_dictionaries_from_csv(source_file):
    with open(source_file, 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        dictionaries = []
        for count, value in enumerate(csv_records):
            if count == 0:
                headers = value
            else:
                row = value
                dictionary = {}
                for i in range(len(headers)):
                    dictionary[headers[i]] = row[i]
                    dictionaries.append(dictionary)
    return dictionaries


if __name__ == "__main__":
    engine = create_engine('sqlite:///stations.db')

    meta = MetaData()
    conn = engine.connect()

    stations_dictionaries = create_dictionaries_from_csv('clean_stations.csv')
    print(stations_dictionaries)
    stations_keys = list(stations_dictionaries[0].keys())

    stations = Table(
        'stations', meta,
        Column(stations_keys[0], String, primary_key=True),
        Column(stations_keys[1], String),
        Column(stations_keys[2], String),
        Column(stations_keys[3], String),
        Column(stations_keys[4], String),
        Column(stations_keys[5], String),
        Column(stations_keys[6], String)
    )
    meta.create_all(engine)

    for item in stations_dictionaries:
        keys = list(item.keys())
        conn.execute(stations.insert(), {keys[0]: item[keys[0]], keys[1]: item[keys[1]], keys[2]: item[keys[2]],
                     keys[3]: item[keys[3]], keys[4]: item[keys[4]], keys[5]: item[keys[5]], keys[6]: item[keys[6]]})

    measure_dictionaries = create_dictionaries_from_csv('clean_measure.csv')
    measure_keys = list(measure_dictionaries[0].keys())

    measure = Table(
        'measure', meta,
        Column(measure_keys[0], String),
        Column(measure_keys[1], String),
        Column(measure_keys[2], String),
        Column(measure_keys[3], String)
    )
    meta.create_all(engine)

    for item in stations_dictionaries:
        keys = list(item.keys())
        conn.execute(measure.insert(), {keys[0]: item[keys[0]], keys[1]: item[keys[1]], keys[2]: item[keys[2]],
                                        keys[3]: item[keys[3]]})
