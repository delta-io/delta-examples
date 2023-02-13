# delta-examples

This repo provides notebooks with Delta Lake examples using PySpark, Scala Spark, and Python.

Running these commands on your local machine is a great way to learn about how Delta Lake works.

## PySpark setup

You can install PySpark and Delta Lake by creating the `pyspark-330-delta-220` conda environment.

Create the environment with this command: `conda env create -f envs/pyspark-330-delta-220`.

Activate the environment with this command: `conda activate pyspark-330-delta-220`.

Then you can run `jupyter lab` and execute all the PySpark notebooks.

## delta-rs setup (Python bindings)

You can run the delta-rs notebooks that use the Python bindings by creating the `mr-delta-rs` conda environment.

Create the environment with this command: `conda env create -f envs/mr-delta-rs.yml`.

Activate the environment with this command: `conda activate mr-delta-rs`.

## Scala setup

You can install [almond](https://almond.sh/) to run the Scala Spark notebooks in this repo.

## Contributing

We welcome contributions, especially notebooks that illustrate important functions that will benefit the Delta Lake community.

Check out the open issues for ideas on good notebooks to create!

