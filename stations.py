import sqlalchemy
import csv
from sqlalchemy import Table, Column, Float, String, Integer, Date, MetaData, ForeignKey, create_engine


def create_headers(source_file):
    with open(source_file, 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        for count, value in enumerate(csv_records):
            if count == 0:
                headers = value
                return headers


def create_values(source_file):
    with open(source_file, 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        row = []
        values = []
        for count, value in enumerate(csv_records):
            if count != 0:
                row = value
                values.append(row)
    return values


if __name__ == "__main__":
    engine = create_engine('sqlite:///stations.db')
    meta = MetaData()
    conn = engine.connect()

    stations_headers = list(create_headers('clean_stations.csv'))
    stations = Table(
        'stations', meta,
        Column(stations_headers[0], String, primary_key=True),
        Column(stations_headers[1], String),
        Column(stations_headers[2], String),
        Column(stations_headers[3], String),
        Column(stations_headers[4], String),
        Column(stations_headers[5], String),
        Column(stations_headers[6], String)
    )
    meta.create_all(engine)

    stations_rows = create_values('clean_stations.csv')

    for item in stations_rows:
        conn.execute(stations.insert(), [{stations_headers[0]: item[0], stations_headers[1]: item[1], stations_headers[2]: item[2],
                                          stations_headers[3]: item[3], stations_headers[4]: item[4], stations_headers[5]: item[5], stations_headers[6]: item[6]}])

    measure_headers = list(create_headers('clean_measure.csv'))
    measure = Table(
        'measure', meta,
        Column(stations_headers[0], String),
        Column(stations_headers[1], String),
        Column(stations_headers[2], String),
        Column(stations_headers[3], String)
    )
    meta.create_all(engine)

    measure_rows = create_values('clean_stations.csv')

    for item in measure_rows:
        conn.execute(stations.insert(), [
                     {measure_headers[0]: item[0], measure_headers[1]: item[1], measure_headers[2]: item[2], measure_headers[3]: item[3]}])
