## Data Driven Spark (spawncamping-dds) [![Build Status](https://travis-ci.org/FRosner/spawncamping-dds.svg?branch=master)](https://travis-ci.org/FRosner/spawncamping-dds)
### Description

This library provides a comprehensible and simple interface for quick data exploration based on 
[Apache Spark](https://spark.apache.org/). The target audience is data scientists who miss functions
like `summary()` and `plot()` from [R](http://www.r-project.org/) when working on the cluster. It does
not offer a fully flexible plotting mechanism like [ggplot2](http://ggplot2.org/) but focuses on giving
you quick insights in your data.

### Usage

1. Add spawncamping-dds jar to Spark shell classpath

    ```sh
    ./bin/spark-shell --jars spawncamping-dds-1.0.0-alpha.jar
    ```
2. Import core functions

    ```scala
    import de.frosner.dds.core.DDS._
    ```
    
3. Start the chart server + web interface

    ```scala
    start()
    ```
    
4. Explore your data

    ```scala
    val grades = sc.parallelize(List(1,2,1,1,2,4,5))
    summary(grades)
    histogram(grades)
    ```
    
### Get Data Driven Spark

You can either grab the [latest release artifact](https://github.com/FRosner/spawncamping-dds/releases) or build from source (`sbt assembly`).

### Contribution

Any contribution, e.g. in form of feature requests, comments, code reviews, pull requests are very welcome. Pull requests will be reviewed before they are merged and it makes sense to coordinate with one of the main committers before starting to work on something big. 

Please follow the general code style convention of Scala. It is advised to stick to the formatting / code style of the surrounding code when making changes to existing files. Reformatting should be done is separate commits.

All (most of the) code committed should be covered by some automated unit tests. All existing tests need to pass before committing changes.

### Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
    
### Licensing

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
