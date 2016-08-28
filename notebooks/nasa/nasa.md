

## Nasa example

**Question**

What were the endpoints most accessed between 20:00 and 23:59?

**Solution**


**Step 1**: Install apache-log-parser lib:
```bash
pip install apache-log-parser
```
After installation, reboot jupyter.

**Step 2**: Set max executor memory.


```python
sc._conf.set("spark.executor.memory", "8g")
```




    <pyspark.conf.SparkConf at 0x7fe446628e48>



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
    return len(line) > 50 and \
            'image' not in line and \
            'icon' not in line and \
            '.gif' not in line

def to_log_line(line):
    parsed_line = parser(line)
    return (parsed_line['remote_host'], \
            parsed_line['time_received_datetimeobj'], \
            parsed_line['request_first_line'].replace('GET', '').replace('HTTP/1.0',''), \
            int(parsed_line['status']), \
            int(parsed_line['response_bytes_clf'].replace('-','0')))    

line_tuples = lines.filter(is_valid).map(to_log_line).cache()

print('Line tuple: %s.' % line_tuples.take(1))
```

    Line tuple: [('199.72.81.55', datetime.datetime(1995, 7, 1, 0, 0, 1), ' /history/apollo/ ', 200, 6245)].


**Step 4**: Get log rows.


```python
from pyspark.sql.types import *
from pyspark.sql import *
from pyspark.sql.functions import *

LogRow = StructType([  StructField('remote_host',   StringType(),    False), \
                       StructField('timestamp',     TimestampType(), False), \
                       StructField('endpoint',      StringType(),    False), \
                       StructField('status',        IntegerType(),   False), \
                       StructField('response_size', LongType(),      False), ])

rows = sqlCtx.createDataFrame(line_tuples, LogRow).cache()

rows.show(1, False)
```

    +------------+---------------------+------------------+------+-------------+
    |remote_host |timestamp            |endpoint          |status|response_size|
    +------------+---------------------+------------------+------+-------------+
    |199.72.81.55|1995-07-01 00:00:01.0| /history/apollo/ |200   |6245         |
    +------------+---------------------+------------------+------+-------------+
    only showing top 1 row
    


**Step 5**: Get endpoints most accessed between 20:00 and 23:59.


```python
most_accessed_endpoints = rows.where('hour(timestamp) >= 20 and hour(timestamp) <= 23 and status = 200') \
    .groupBy('endpoint') \
    .count() \
    .orderBy(desc('count'))

most_accessed_endpoints.show(20, False)
```

    +-------------------------------------------------+-----+
    |endpoint                                         |count|
    +-------------------------------------------------+-----+
    | /shuttle/countdown/                             |5590 |
    | /                                               |4970 |
    | /ksc.html                                       |4084 |
    | /shuttle/missions/missions.html                 |3617 |
    | /htbin/cdt_main.pl                              |2961 |
    | /shuttle/missions/sts-71/mission-sts-71.html    |2569 |
    | /shuttle/countdown/liftoff.html                 |2569 |
    | /history/apollo/apollo-13/apollo-13.html        |2489 |
    | /history/apollo/apollo.html                     |2476 |
    | /shuttle/missions/sts-70/mission-sts-70.html    |2264 |
    | /history/history.html                           |1911 |
    | /shuttle/countdown/countdown.html               |1195 |
    | /software/winvn/winvn.html                      |1122 |
    | /shuttle/technology/sts-newsref/stsref-toc.html |1108 |
    | /history/apollo/apollo-13/apollo-13-info.html   |1101 |
    | /shuttle/missions/sts-71/movies/movies.html     |946  |
    | /htbin/cdt_clock.pl                             |906  |
    | /facilities/lc39a.html                          |904  |
    | /shuttle/missions/sts-69/mission-sts-69.html    |892  |
    | /shuttle/missions/sts-70/movies/movies.html     |871  |
    +-------------------------------------------------+-----+
    only showing top 20 rows
    

