Make the most of the Datastax Cassandra driver for python by tuning it correctly.

# Quick example

Throughout this tutorial we'll use a Cassandra table called time_series to store a simple time series. Here's the code to set up the table:
```cassandraql

CREATE KEYSPACE fastsandra
  WITH REPLICATION = { 
   'class' : 'SimpleStrategy', 
   'replication_factor' : 1 
  };


CREATE TABLE fastsandra.time_series ( 
  event_date date,
  instrument_id int,
  event_timestamp timestamp,
  value double,
  PRIMARY KEY (event_date, instrument_id, event_timestamp)
);

```

And here's how to query the data for given date, using [Cluster.execute](https://docs.datastax.com/en/developer/python-driver/3.20/api/cassandra/cluster/#cassandra.cluster.Session-methods):

```python
import cassandra.cluster

cluster = cassandra.cluster.Cluster() # default cluster, pointing to localhost

with cluster.connect('fastsandra') as session:    
    results = session.execute(
        "SELECT * from fastsandra.time_series where event_date = '2019-10-01'"
    )
    rows = [r for r in results]
rows[0]
# Row(event_date=Date(18170), instrument_id=1, event_timestamp=datetime.datetime(2019, 10, 1, 8, 0), value=0.2210726153252428)
rows[0].value
# 0.2210726153252428
```

The driver returns a bunch of rows, each of them is a [named tuple](https://docs.python.org/3/library/collections.html#collections.namedtuple). 

It's very easy to convert the results into a [pandas DataFrame](https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.html):
```python
import pandas as pd
pd.DataFrame(rows)
```

```bash
   event_date  instrument_id     event_timestamp     value
0  2019-10-01              1 2019-10-01 08:00:00  0.221073
1  2019-10-01              1 2019-10-01 08:15:00  0.661251
2  2019-10-01              1 2019-10-01 08:30:00  0.927390
3  2019-10-01              1 2019-10-01 08:45:00  0.083483
4  2019-10-01              1 2019-10-01 09:00:00  0.934817

```

# Speed up the driver with `NumpyProtocolHandler`

If you're dealing with a lot of data, there is a big performance and memory overhead when using named tuples and plain python objects (int, float etc). It's much better to use numpy arrays and cython for this. 

The good news is that you can set up the Cassandra driver so it reads the data directly into numpy array, using cython, and avoiding the named tuple/python object overhead.

## Installing the cython version of the Cassandra driver with `NumpyProtocolHandler`

First you need to install the driver correctly. Here's how to do it on ubuntu, for python 3.7:

```bash
# First make sure you have your the correct library installed on your system:
sudo apt install python3.7-dev libev4 libev-dev build-essential
# Activate your your virtual environment and first install Cython and numpy
pip install Cython==0.29.14
pip install numpy==0.17.2
# Then install the cassandra-driver (this should take a few minutes)
pip install cassandra-driver
# Check that it worked:
/home/arthur/projects/fastsandra-git/BLOG.md
python -c 'from cassandra.protocol import NumpyProtocolHandler;print(NumpyProtocolHandler)'
rom cassandra.protocol import NumpyProtocolHandler
# Should print:
# <class 'cassandra.protocol.cython_protocol_handler.<locals>.CythonProtocolHandler'>
# If it doesn't print anything, it didn't work
```

This can be a bit tricky so here are some trouble shooting tips:
* You must have the python development library installed
* You must install Cython before you install the driver
* If you installed cassandra-driver previously you may have to clear your pip cache
* Another option is to use `pip --no-cache-dir `
* If it still does not work, run the installation with `-v` and check the logs
```bash
pip -v --no-cache-dir install cassandra-driver
```

## Using `NumpyProtocolHandler`

```python
from cassandra.protocol import NumpyProtocolHandler
from cassandra.query import tuple_factory

with cluster.connect('fastsandra') as session:
    session.row_factory = tuple_factory  #required for Numpy results
    session.client_protocol_handler = NumpyProtocolHandler  # for a dict of NumPy arrays as result
    results = session.execute(
        "SELECT * from fastsandra.time_series where event_date = '2019-10-01'"
    )
    np_rows = [r for r in results]
df = pd.concat([pd.DataFrame(r) for r in rows])

```

With [Session client_protocol_handler](https://docs.datastax.com/en/drivers/python/3.2/api/cassandra/cluster.html#cassandra.cluster.Session.client_protocol_handler) set to `NumpyProtocolHandler` the content of the result set is changed. 
Instead of a sequence of named tuples, it is now a sequence of dict of [numpy ndarray](https://docs.scipy.org/doc/numpy/reference/generated/numpy.ndarray.html). 
For example `np_rows[0]['values']` is a [masked array](https://docs.scipy.org/doc/numpy/reference/maskedarray.generic.html) of `double`:
```python
masked_array(data=[0.2210726153252428, 0.6612507337531311,
                   0.9273900637252853, ..., 0.1700777337201711,
                   0.6348330019120819, 0.23939705731588268],
             mask=[False, False, False, ..., False, False, False],
       fill_value=1e+20)
```

And `np_rows[0]['instrument_id']` is a [masked array](https://docs.scipy.org/doc/numpy/reference/maskedarray.generic.html) of `int32`:

```python
masked_array(data=[1, 1, 1, ..., 136, 136, 136],
             mask=[False, False, False, ..., False, False, False],
       fill_value=999999,
            dtype=int32)
```

This means that we save a lot of memory and CPU time by not having the intermediate objects (integer, float, named tuple).

# Tuning the driver further

Numpy is much faster when the data type of the array is a native type: double, integer and timestamp. 
On the other hand it is very slow when the data type is `object`. 
Unfortunately the cassandra driver doesn't have support timestamp.
Our date and timestamp columns are objects:
```python
np_rows[0]['event_date'].dtype
# dtype('O')
np_rows[0]['event_timestamp'].dtype
# dtype('O')
```

But there is an easy way to [monkey patch](https://en.wikipedia.org/wiki/Monkey_patch) the driver so it supports datetime (and date).

[numpy_parser](https://github.com/datastax/python-driver/blob/master/cassandra/numpy_parser.pyx) defines mapping between the cassandra and numpy data types 
```python
_cqltype_to_numpy = {
    cqltypes.LongType:          np.dtype('>i8'),
    cqltypes.CounterColumnType: np.dtype('>i8'),
    cqltypes.Int32Type:         np.dtype('>i4'),
    cqltypes.ShortType:         np.dtype('>i2'),
    cqltypes.FloatType:         np.dtype('>f4'),
    cqltypes.DoubleType:        np.dtype('>f8'),
}
``` 
All we need to do is add support for date and timestamp.

# Add support for timestamps

```python
import cassandra.cqltypes
import cassandra.numpy_parser as numpy_parser


numpy_parser._cqltype_to_numpy.update({
    cassandra.cqltypes.DateType: np.dtype('datetime64[ms]'),
    cassandra.cqltypes.TimestampType: np.dtype('datetime64[ms]'),
})
```

And if we reload the data from cassandra we can see:
```python
np_rows[0]['event_timestamp'].dtype
# dtype('>M8[ms]')
```

This is very easy because the behind the scene Cassandra represent timestamp as the number of millis since epoch, the same way numpy does.   


# Add support for dates

For dates it requires more work. Because of the way Cassandra represents date:
```python
# Values of the 'date'` type are encoded as 32-bit unsigned integers
# representing a number of days with epoch (January 1st, 1970) at the center of the
# range (2^31).
```

When loading date fields we need to do some massaging: 
* load unsigned int (32bit) 
* withdraw the `EPOCH_OFFSET` (2^31)
* Convert to `datetime64[D]`

```python
numpy_parser._cqltype_to_numpy.update({
  cassandra.cqltypes.SimpleDateType: np.dtype('>u4'),
})

def result_set_to_df(results: cassandra.cluster.ResultSet) -> pd.DataFrame:
    df = pd.DataFrame(pd.concat((pd.DataFrame(r) for r in  results)))
    for name, dtype in zip(results.column_names, results.column_types):
        if dtype == cassandra.cqltypes.SimpleDateType:
            df[name] = (df[name] - cassandra.cqltypes.SimpleDateType.EPOCH_OFFSET_DAYS).astype('datetime64[D]')
    return df
```

And you can run this example:
```python
with cluster.connect('fastsandra') as session:
    session.row_factory = tuple_factory
    session.client_protocol_handler = NumpyProtocolHandler

    results = session.execute(
        "SELECT * from fastsandra.time_series where event_date = '2019-10-01'"
    )
    df = result_set_to_df(results)
```
The resulting DataFrame has got the following dtypes:
```python
event_date         datetime64[ns]
instrument_id               int32
event_timestamp    datetime64[ns]
value                     float64
```

There is zero object overhead, everything goes straight from the wire format to numpy arrays.


