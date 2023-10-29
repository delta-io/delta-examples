# delta-examples

This repo provides notebooks with Delta Lake examples using PySpark, Scala Spark, and Python.

Running these commands on your local machine is a great way to learn about how Delta Lake works.

## PySpark setup

You can install PySpark and Delta Lake by creating the `pyspark-330-delta-220` conda environment.

Create the environment with this command: `conda env create -f envs/pyspark-330-delta-220`.

Activate the environment with this command: `conda activate pyspark-330-delta-220`.

Then you can run `jupyter lab` and execute all the PySpark notebooks.

## Release Candidate setup

The Delta Lake Spark connector release process typically includes an early release candidate release a few weeks prior to the official release. ([example](https://github.com/delta-io/delta/releases/tag/v2.4.0rc1))

The release candidate is typically hosted in non-standard repositories that require additional setup.

To configure a notebook to use a release candidate:

1. Make sure to use a conda environment that includes the correct `--extra-index-url`, see [here](https://github.com/delta-io/delta-examples/blob/master/envs/pyspark-340-delta-240rc1.yml#L19-L20) for an example.
2. Provide the appropriate Ivy settings file that has the correct non-standard repository, see [this one](https://github.com/delta-io/delta-examples/blob/master/ivy/2.4.0rc1.xml) for an example.
3. Initialize Spark with the correct Ivy configuration

    ```
    .config(
        "spark.jars.ivySettings",
        "../../ivy/2.4.0rc1.xml"
    )
    ```

## delta-rs setup (Python bindings)

You can run the delta-rs notebooks that use the Python bindings by creating the `mr-delta-rs` conda environment.

Create the environment with this command: `conda env create -f envs/mr-delta-rs.yml`.

Activate the environment with this command: `conda activate mr-delta-rs`.

## delta-rs Rust setup

Rust notebooks in `notebooks/delta-rs` were developed using [Evcxr Jupyter Kernel](https://github.com/evcxr/evcxr/tree/main/evcxr_jupyter). 

You can either follow the instructions in [Evcxr Jupyter Kernel](https://github.com/evcxr/evcxr/tree/main/evcxr_jupyter) to set it up, or you can use the included Docker file using the following commands (in which case all you need is Docker installed):
```
cd notebooks/delta-rs

docker build -t delta-rs .
docker run -it --rm  -p 8888:8888 --name delta-rs -v $PWD/notebooks/delta-rs:/usr/src/delta-rs delta-rs
```

**Note:** *One of the main reasons for creating the Dockerfile was an issue with running Evcxr Jupyter Kernel on MacOS with Applle chip - despite being able to build Rust applications directly on on the host after setting `[target.aarch64-apple-darwin]` in the `~/.cargo/config` file, `:dep` builds are stil not working in the notebook.* 


## Scala setup

You can install [almond](https://almond.sh/) to run the Scala Spark notebooks in this repo.

## Contributing

We welcome contributions, especially notebooks that illustrate important functions that will benefit the Delta Lake community.

Check out the open issues for ideas on good notebooks to create!

