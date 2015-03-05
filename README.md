## Data-Driven Spark (spawncamping-dds) [![Build Status](https://travis-ci.org/FRosner/spawncamping-dds.svg?branch=master)](https://travis-ci.org/FRosner/spawncamping-dds)
### Description

This library provides a comprehensible and simple interface for quick data exploration based on 
[Apache Spark](https://spark.apache.org/) and [D3.js/SVG](http://d3js.org/). The target audience is
data scientists who miss functions like `summary()` and `plot()` from [R](http://www.r-project.org/)
when working on the cluster. It does not offer a fully flexible plotting mechanism like [ggplot2](http://ggplot2.org/) but focuses on giving you quick insights into your data.

### Usage

1. Add spawncamping-dds jar to Spark shell classpath

    ```sh
    ./bin/spark-shell --driver-class-path spawncamping-dds-x.y.z.jar
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
    summarize(grades)
    ```
    
5. Stop the server once you are done

    ```scala
    stop()
    ```
    
See the [User Guide](https://github.com/FRosner/spawncamping-dds/wiki/User-Guide) for a detailed explanation of the provided functionality.
    
### Get Data-Driven Spark

You can either grab the [latest release artifact](https://github.com/FRosner/spawncamping-dds/releases) or build from source (`sbt assembly`). Data-Driven Spark can be cross built against Scala version 2.10.4 and 2.11.2.

### Contribution

Any contribution, e.g. in form of feature requests, comments, code reviews, pull requests are very welcome. Pull requests will be reviewed before they are merged and it makes sense to coordinate with one of the main committers before starting to work on something big. 

Please follow the general code style convention of Scala. It is advised to stick to the formatting / code style of the surrounding code when making changes to existing files. Reformatting should be done is separate commits.

All (most of the) code committed should be covered by some automated unit tests. All existing tests need to pass before committing changes.

### Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
- [Rick Moritz](https://github.com/RPCMoritz) (Contributor)
    
### Licensing

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.

### Included Libraries

| Library       | License        |
| ------------  | -------------- |
| [spray](http://spray.io/) | Apache 2 |
| [D3.js](http://d3js.org/) | Custom |
| [C3.js](http://c3js.org/) | MIT |
| [jQuery](http://jquery.com/) | Custom (MITish) |
