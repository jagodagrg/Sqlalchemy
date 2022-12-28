import sqlalchemy
import csv
from sqlalchemy import Table, Column, String, Float, Integer, Date, MetaData, ForeignKey, create_engine


def create_dictionaries_from_csv(source_file):
    with open(source_file, 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        dictionaries = []
        for count, value in enumerate(csv_records):
            if count == 0:
                headers = value
            else:
                dictionaries.append(dict(zip(headers, value)))
    return dictionaries


def select_row(table, station_id):
    if table == 'stations':
        s = stations.select().where(stations.c.station == station_id)
    else:
        s = measure.select().where(measure.c.station == station_id)
    results = conn.execute(s).fetchall()
    return results


def update_name(table, station_id, new_name):
    if table == 'stations':
        s = stations.update().\
            values(name=new_name).\
            where(stations.c.station == station_id)
    else:
        s = measure.update().\
            values(name=new_name).\
            where(measure.c.station == station_id)
    conn.execute(s)


def delete(table, station_id):
    if table == 'stations':
        d = stations.delete().\
            where(stations.c.station == station_id)
    else:
        d = measure.delete().\
            where(measure.c.station == station_id)
    conn.execute(d)


if __name__ == "__main__":
    engine = create_engine('sqlite:///stations.db')
    meta = MetaData()
    conn = engine.connect()

    stations_dictionaries = create_dictionaries_from_csv('clean_stations.csv')
    stations = Table(
        'stations', meta,
        Column('station', String, primary_key=True),
        Column('latitude', String),
        Column('longitude', String),
        Column('elevation', String),
        Column('name', String),
        Column('country', String),
        Column('state', String)
    )
    meta.create_all(engine)
    conn.execute(stations.insert(), stations_dictionaries)

    measure_dictionaries = create_dictionaries_from_csv('clean_measure.csv')
    measure = Table(
        'measure', meta,
        Column('station', String, ForeignKey('stations.station')),
        Column('date', String),
        Column('precip', String),
        Column('tobs', Integer)
    )
    meta.create_all(engine)
    conn.execute(measure.insert(), measure_dictionaries)
