
## Install Spark & Jupyter on Arch Linux

**Step 1:** Install spark.

```bash
yaourt -S apache-spark
```

**Step 2:** Install Anaconda/Jupyter.

```bash
yaourt -S anaconda
```

**Step 3:** Edit ```~/.bashrc``` and add:

```bash
export SPARK_HOME=/opt/apache-spark
export PYTHONPATH=$SPARK_HOME/python

export ANACONDA_ROOT=/opt/anaconda
export PYSPARK_DRIVER_PYTHON=$ANACONDA_ROOT/bin/ipython
export PYSPARK_PYTHON=$ANACONDA_ROOT/bin/python

export PATH=$ANACONDA_ROOT/bin:$PATH
export PATH=$SPARK_HOME/bin:$PATH

alias jupyter='IPYTHON_OPTS="notebook" pyspark'
```

**Step 4:** Include variables on previous step.

```bash
source ~/.bashrc
```

**Step 5:** Start Jupyter.

```bash
jupyter
```

**Step 6:** Go to [http://localhost:8888](http://localhost:8888).

## Notebooks

* [Shakespeare example](notebooks/shakespeare/shakespeare.md)
* [Shakespeare example (Using a schema and sql)](notebooks/shakespeare/shakespeare-v2.md)
