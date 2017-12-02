
## Install Spark & Jupyter on Arch Linux

**Step 1:** Install spark.

```bash
yaourt -S apache-spark
```

**Step 2:** Install Anaconda/Jupyter.

```bash
yaourt -S anaconda
```

**Step 3:** Create ```~/.jupyterrc``` and add:

```bash
# Tell spark that use all cpu cores...
export MASTER=local[*]
export SPARK_HOME=/opt/apache-spark
export PYTHONPATH=$SPARK_HOME/python

export ANACONDA_ROOT=/opt/anaconda
export PYSPARK_DRIVER_PYTHON=$ANACONDA_ROOT/bin/ipython
export PYSPARK_PYTHON=$ANACONDA_ROOT/bin/python

export PATH=$ANACONDA_ROOT/bin:$PATH
export PATH=$SPARK_HOME/bin:$PATH

alias jupyter='PYSPARK_DRIVER_PYTHON_OPTS="notebook" pyspark'
```

**Step 4:** Include .jupyterrc under shell startup script.

```bash
echo "source ~/.jupiterrc" >> ~/.bashrc
```

**Step 5:** Include variables in current shell session.

```bash
source ~/.jupiterrc
```

**Step 6:** Start Jupyter.

```bash
jupyter
```

**Step 7:** Go to [http://localhost:8888](http://localhost:8888).

## Notebooks

* [Shakespeare example](notebooks/shakespeare/shakespeare.md)
* [Shakespeare example (Using a schema and sql)](notebooks/shakespeare/shakespeare-v2.md)
* [Nasa](notebooks/nasa/nasa.md)
