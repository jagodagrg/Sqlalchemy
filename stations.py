import sqlalchemy
import csv
from sqlalchemy import Table, Column, Float, String, Integer, Date, MetaData, ForeignKey, create_engine


if __name__ == "__main__":

    engine = create_engine('sqlite:///stations.db')

    meta = MetaData()

    stations = Table(
        'stations', meta,
        Column('station', String, primary_key=True),
        Column('latitude', String),
        Column('longtitude', String),
        Column('elevation', String),
        Column('name', String),
        Column('country', String),
        Column('state', String)
    )

    measures = Table(
        'measures', meta,
        Column('station', String, ForeignKey('stations.station')),
        Column('date', String),
        Column('precip', String),
        Column('tobs', Integer)
    )

    meta.create_all(engine)

    with open('clean_stations.csv', 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        ins = stations.insert()
        conn = engine.connect()
        for row in csv_records:
            conn.execute(ins, [{'station': row[0], 'latitude': row[1], 'longtitude': row[2],
                         'elevation': row[3], 'name': row[4], 'country': row[5], 'state': row[6]}])

    with open('clean_measure.csv', 'r') as csv_file:
        csv_records = csv.reader(csv_file, delimiter=',')
        ins = measures.insert()
        conn = engine.connect()
        for row in csv_records:
            conn.execute(
                ins, [{'station': row[0], 'date': row[1], 'precip': row[2], 'tobs': row[3]}])
