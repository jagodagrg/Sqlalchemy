import sqlalchemy
import csv
from sqlalchemy import Table, Column, String, Float, Integer, Date, MetaData, ForeignKey, create_engine, update


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

    # testy modyfikacji
    s = stations.select().where(stations.c.station == "USC00519397")
    results = conn.execute(s).fetchall()
    print(results)

    stmt = stations.update().\
        values(name='Waikiki 123').\
        where(stations.c.station == 'USC00519397')
    conn.execute(stmt)
    s = stations.select().where(stations.c.station == "USC00519397")
    results = conn.execute(s).fetchall()
    print(results)

    stmt = stations.delete().\
        where(stations.c.station == 'USC00513117')
    conn.execute(stmt)
    s = stations.select().where(stations.c.station == "USC00513117")
    results = conn.execute(s).fetchall()
    print(results)
