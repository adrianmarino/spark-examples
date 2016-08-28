

## Nasa example


**Step 1**: Before all, download [nasa logs](http://ita.ee.lbl.gov/html/contrib/NASA-HTTP.html).

**Step 2**: Load logs and show lines count.


```python
logs = sc.textFile('/home/adrian/development/spark/notebooks/nasa/access_log_Jul95')

print('Logs count: %s.' % logs.count())
```

    Logs count: 1891715.


**Step 3**: What are the enpoints most accessed between 20:00 and 23:59?


```python
# In Progress...
```
