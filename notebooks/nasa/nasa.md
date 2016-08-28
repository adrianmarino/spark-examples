

## Nasa example

**Question**: What are the endpoints most accessed between 20:00 and 23:59?


**Step 1**: Install apache-log-parser lib:
```bash
pip install apache-log-parser
```
After installation, reboot jupyter.

**Step 2**: Set max executor memory.


```python
sc._conf.set("spark.executor.memory", "8g")
```




    <pyspark.conf.SparkConf at 0x7f248222efd0>



**Step 3**: Download [nasa logs](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html) used as input of this example.

**Step 4**: Load logs and show lines count.


```python
lines = sc.textFile('/home/adrian/development/spark/notebooks/nasa/access_log_Jul95').cache()

print('First Line: %s' % lines.take(1))
print('File size: %s lines.' % lines.count())
```

    First Line: ['199.72.81.55 - - [01/Jul/1995:00:00:01 -0400] "GET /history/apollo/ HTTP/1.0" 200 6245']
    File size: 1891715 lines.


**Step 3**: Get log lines in tuples.


```python
import sys
from py4j.protocol import Py4JJavaError
import apache_log_parser

parser = apache_log_parser.make_parser('%h - - %t \"%r\" %s %b')

def is_valid(line):
    return len(line) > 50

def to_log_line(line):
    parsed_line = parser(line)
    return (parsed_line['remote_host'], \
            parsed_line['time_received_datetimeobj'], \
            parsed_line['request_first_line'], \
            int(parsed_line['status']), \
            int(parsed_line['response_bytes_clf'].replace('-','0')))        

line_tuples = lines.filter(is_valid).map(to_log_line).cache()

print('Line tuple: %s.' % line_tuples.take(1))
```

    Line tuple: [('199.72.81.55', datetime.datetime(1995, 7, 1, 0, 0, 1), 'GET /history/apollo/ HTTP/1.0', 200, 6245)].


**Step 4**: Get log rows.


```python
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

LogRow = StructType([  StructField('remote_host',   StringType(),    False), \
                       StructField('timestamp',     TimestampType(), False), \
                       StructField('request',       StringType(),    False), \
                       StructField('status',        IntegerType(),   False), \
                       StructField('response_size', LongType(),      False), ])

rows = sqlCtx.createDataFrame(line_tuples, LogRow).cache()

rows.show(1)
```

    +------------+--------------------+--------------------+------+-------------+
    | remote_host|           timestamp|             request|status|response_size|
    +------------+--------------------+--------------------+------+-------------+
    |199.72.81.55|1995-07-01 00:00:...|GET /history/apol...|   200|         6245|
    +------------+--------------------+--------------------+------+-------------+
    only showing top 1 row
    


**Step 5**: Get rows between 20:00 and 23:59.


```python
# In Progress...
```

**Step 6**: Get endpoints most accessed.


```python
# In Progress...
```
