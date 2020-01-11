import random

import pandas as pd
from cassandra import ConsistencyLevel
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement

FREQUENCY = '15min'
INSTRUMENTS = 1000
DATES = pd.bdate_range(pd.Timestamp('2019-10-01'), pd.Timestamp('2019-10-30'))


def get_days_data(date):
    return [
        (date, instrument_id, timestamp, random.random())
        for instrument_id in range(1, INSTRUMENTS)
        for timestamp in pd.date_range(
            date + pd.Timedelta('8h'),
            date + pd.Timedelta("17h"),
            freq=FREQUENCY)
    ]


def get_all_data():
    data = []
    for date in DATES:
        data.extend(get_days_data(date))
    return data


def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(l), n):
        yield l[i:i + n]


def populate_data(cluster, data):
    with cluster.connect('fastsandra') as session:
        query = (
            "INSERT INTO time_series"
            " (event_date, instrument_id, event_timestamp, value)"
            "VALUES (?, ?, ?, ?)"
        )
        insert_statement = session.prepare(query)

        for chunck in chunks(data, 142):
            batch = BatchStatement(consistency_level=ConsistencyLevel.QUORUM)
            for record in chunck:
                batch.add(insert_statement, record)
            session.execute(batch)


def main():
    data = get_all_data()
    populate_data(Cluster(), data)


if __name__ == '__main__':
    main()
