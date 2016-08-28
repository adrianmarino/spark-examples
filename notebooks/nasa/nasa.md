

## Nasa example

**Question**: What are the endpoints most accessed between 20:00 and 23:59?


**Step 1**: Before all, download [nasa logs](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html).

**Step 2**: Load logs and show lines count.


```python
lines = sc.textFile('/home/adrian/development/spark/notebooks/nasa/access_log_Jul95')

print('Lines: %s.' % lines.count())
```

    Lines: 1891715.


**Step 3**: Get line tuple.


```python
import time
import datetime

def to_timestamp(string):
    return datetime.datetime.strptime(string, '%d/%b/%Y:%H:%M:%S %z')

def to_line_tuple(line):
    line_part = line.replace('[', ',').replace(']',',').split(',')
    return (line_part[0].replace('-','').strip(), to_timestamp(line_part[1]), line_part[2].strip())


line_tuples = lines.map(lambda line: to_line_tuple(line)).cache()
    
print('Line tuple: %s.' % line_tuples.take(1))
```

    Line tuple: [('199.72.81.55', datetime.datetime(1995, 7, 1, 0, 0, 1, tzinfo=datetime.timezone(datetime.timedelta(-1, 72000))), '"GET /history/apollo/ HTTP/1.0" 200 6245')].


**Step 4**: Get log rows.


```python
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

LogRow = StructType([  StructField('remote_host', StringType(),    False), \
                       StructField('datetime',    TimestampType(), False), \
                       StructField('message',     StringType(),    False)  ])
    
rows = sqlCtx.createDataFrame(line_tuples, LogRow)

rows.show(1)
```

    +------------+--------------------+--------------------+
    | remote_host|            datetime|             message|
    +------------+--------------------+--------------------+
    |199.72.81.55|1995-07-01 01:00:...|"GET /history/apo...|
    +------------+--------------------+--------------------+
    only showing top 1 row
    


**Step 5**: Get rows between 20:00 and 23:59.


```python
# In Progress...
```

**Step 6**: Get endpoints most accessed.


```python
# In Progress...
```
