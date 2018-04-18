# About
MapReduce algorithm for counting words in .txt and .csv file which support number of mappers, combiners and reducers.
Also, you can choose to use combiners or not.

# Tech stack
* Python 3.5

# Get started

### Clone git

    ~$ git clone https://github.com/PankivAndrew/MapReduce

### Usage
It is easy set number of mappers, combiners and reducers (default value is 10)

#### With combiners

MapReduce(file_path, num_mappers=10, num_reducers=10, num_combiners=10, combine=False)
```python
from map_reduce import MapReduce

map_reduce = MapReduce('test_data/test.txt', 12, 12, combine=True).get_result()
```
Framework will run MapReduce algorithm with 12 mappers, 12 reducers, 10 combiners and create .log file for you.

#### Without combiners

```python
from map_reduce import MapReduce

map_reduce = MapReduce('test_data/test.txt').get_result()
```
Framework will run MapReduce algorithm  with 10 mappers, 10 reducers and create .log file for you.
