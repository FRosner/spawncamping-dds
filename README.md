## Data-Driven Spark (spawncamping-dds) [![Build Status](https://travis-ci.org/FRosner/spawncamping-dds.svg?branch=master)](https://travis-ci.org/FRosner/spawncamping-dds)
### Description

This library provides a comprehensible and simple interface for quick data exploration based on
[Apache Spark](https://spark.apache.org/) and [D3.js/SVG](http://d3js.org/). The target audience is
data scientists who miss functions like `summary()` and `plot()` from [R](http://www.r-project.org/)
when working on the cluster with the Spark REPL. It does not offer a fully flexible plotting mechanism like [ggplot2](http://ggplot2.org/) but focuses on giving you quick insights into your data.

### Usage

1. Add spawncamping-dds jar to Spark classpath

    ```sh
    ./bin/spark-shell --jars spawncamping-dds-<ddsVersion>_<scalaVersion>.jar
    ```
2. Import core functions

    ```scala
    import de.frosner.dds.core.DDS._
    ```

3. Start the web server + user interface

    ```scala
    start()
    ```

4. Explore your data

    ```scala
    // load example data set
    val golf = de.frosner.dds.datasets.golf(sc)

    // look at a sample of your data set
    show(golf)

    // visualize the distribution of a numeric attribute
    histogram(golf.map(_.temperature), buckets = List(60, 65, 70, 75, 80, 85, 90))

    // compute some summary statistics
    summarize(golf.map(_.humidity))

    // visualize the distribution of a nominal attribute
    bar(golf.map(_.outlook))
    ```

5. Stop the server once you are done

    ```scala
    stop()
    ```

See the [User Guide](https://github.com/FRosner/spawncamping-dds/wiki/User-Guide) for a detailed explanation of the provided functionality.

### Get Data-Driven Spark

You can either grab the [latest release artifact](https://github.com/FRosner/spawncamping-dds/releases), use the most recent [SNAPSHOT](http://spawncamping-dds-snapshots.s3-website-us-east-1.amazonaws.com/) or build from source (`sbt build`). Data-Driven Spark (DDS) 2.x is currently developed and built against Spark 1.3. It can be cross built against Scala version 2.10 and 2.11, depending on which version was used to build your Spark. Users of Spark 1.2 need to use the older DDS 1.x releases.

### Contribution

Any contribution, e.g. in form of feature requests, comments, code reviews, pull requests are very welcome. Pull requests will be reviewed before they are merged and it makes sense to coordinate with one of the main committers before starting to work on something big.

Please follow the general code style convention of Scala. It is advised to stick to the formatting / code style of the surrounding code when making changes to existing files. Reformatting should be done in separate commits.

All (most of the) code committed should be covered by some automated unit tests. All existing tests need to pass before committing changes.

Please view the [Developer Guide](https://github.com/FRosner/spawncamping-dds/wiki/Developer-Guide) for additional information about extending DDS.

### Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
- [Milos Krstajic](https://github.com/milosk) (Contributor)
- [Rick Moritz](https://github.com/RPCMoritz) (Contributor)

### Licensing

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.

### Included Libraries

| Library       | License        |
| ------------  | -------------- |
| [spray](http://spray.io/) | Apache 2 |
| [scalaj-http](https://github.com/scalaj/scalaj-http) | Apache 2 |
| [D3.js](http://d3js.org/) | Custom |
| [C3.js](http://c3js.org/) | MIT |
| [Parallel Coordinates](https://github.com/syntagmatic/parallel-coordinates) | Custom |
| [jQuery](http://jquery.com/) | Custom (MITish) |
| [SlickGrid](https://github.com/mleibman/SlickGrid) | MIT |
| [Chroma.js](https://github.com/gka/chroma.js) | BSD |
| [Underscore.js](http://underscorejs.org/) | MIT |
| [Bootstrap](http://getbootstrap.com) | MIT |
