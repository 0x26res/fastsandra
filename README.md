# Fastsandra

## TODO:

- [ ] Clean up the notebook
- [ ] Add a benchmark

## Set up venv on ubuntu

```bash
virtualenv -p virtualenv -p /usr/bin/python3.7 myenv37 --clear 
```

## Installing Cassandra with the `NumpyProtocolHandler`

Instructions are for python 3.7


```bash
# First make sure you have your the correct library installed on your system:
sudo apt install python3.7-dev libev4 libev-dev
# Activate your your virtual environment and first install Cython
pip install Cython==0.29.14
pip install numpy==1.17.1
# Then install the cassandra-driver (this should take a few minutes)
pip install cassandra-driver
# Check that it worked:
from cassandra.protocol import NumpyProtocolHandler
# Should print <class 'cassandra.protocol.cython_protocol_handler.<locals>.CythonProtocolHandler'>
```


Trouble shooting tips:
* Cython must be installed before the driver 
* If you installed cassandra-driver previously you may have to clear your pip cache
* Another option is to use `pip --no-cache-dir `  



## Motivation

The `NumpyProtcolHandler` speed up the processing of numeric columns (int, double...).
It does it by storing their values directly in numpy array, and not using their object representation.
Objects in numpy (and pandas) have got big performance cost (in CPU and memory term).

Unfortunately it does not work for other types, which are stored as object within numpy arrays. 
But this can be extended to date, time and datetime columns, for which numpy has got efficient support.

There are two ways to solve the problem:
* On the surface: Convert the numpy arrays for these specific type when receiving them from the driver
* Withing the driver: trick the driver to treat these columns as numbers and convert the numbers to numpy date type when the driver returns them

While the first solution is simpler, it still has the overhead of creating a lot of short lived objects.
The second solution is much better in terms of performance, and is actually simple to implement


## Example 
 
  

### TODO
[ ] add memory usage plugin
 