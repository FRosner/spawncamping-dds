## Data-Driven Spark [![Build Status](https://travis-ci.org/FRosner/spawncamping-dds.svg?branch=master)](https://travis-ci.org/FRosner/spawncamping-dds) [![Codacy Badge](https://api.codacy.com/project/badge/grade/0a6362a32754458f83a160eef13de7ae)](https://www.codacy.com/app/frank_7/spawncamping-dds) [![codecov.io](https://codecov.io/github/FRosner/spawncamping-dds/coverage.svg?branch=master)](https://codecov.io/github/FRosner/spawncamping-dds?branch=master)
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
2. Import core functions and web UI

    ```scala
    import de.frosner.dds.core.DDS._
    import de.frosner.dds.webui.server.SprayServer._
    ```

3. Start the web server + user interface

    ```scala
    start()
    ```

4. Explore your data

    ```scala
    // load example data set
    val sql = new org.apache.spark.sql.SQLContext(sc)
    val golf = de.frosner.dds.datasets.golf(sql)

    // look at a sample of your data set
    show(golf)

    // compute column statistics
    summarize(golf)

    // visualize column dependencies
    mutualInformation(golf)
    ```

5. Stop the server once you are done

    ```scala
    stop()
    ```

See the [User Guide](https://github.com/FRosner/spawncamping-dds/wiki/User-Guide) for a detailed explanation of the provided functionality.

### Get Data-Driven Spark

You can either grab the [latest release artifact](https://github.com/FRosner/spawncamping-dds/releases), use the most recent [SNAPSHOT](http://spawncamping-dds-snapshots.s3-website-us-east-1.amazonaws.com/) or build from source (`sbt build`). Data-Driven Spark (DDS) 4.x.y is currently developed and built against Spark 1.5. It can be cross built against Scala version 2.10 and 2.11, depending on which version was used to build your Spark. For older versions of Spark, please refer to the following table:

| DDS Versions | Spark Versions |
| --- | --- |
| 4.x.y | 1.5.x |
| 3.x.y | 1.4.x |
| 2.x.y | 1.3.x |
| 1.x.y | 1.2.x |


### Contribution

Any contribution, e.g. in form of feature requests, comments, code reviews, pull requests are very welcome. Pull requests will be reviewed before they are merged and it makes sense to coordinate with one of the main committers before starting to work on something big.

Please follow the general code style convention of Scala. It is advised to stick to the formatting / code style of the surrounding code when making changes to existing files. Reformatting should be done in separate commits.

All (most of the) code committed should be covered by some automated unit tests. All existing tests need to pass before committing changes.

Please view the [Developer Guide](https://github.com/FRosner/spawncamping-dds/wiki/Developer-Guide) for additional information about extending DDS.

### Authors

- [Frank Rosner](https://github.com/FRosner) (Creator)
- [Aleksandr Sorokoumov](https://github.com/Gerrrr) (Contributor)
- [Rick Moritz](https://github.com/RPCMoritz) (Contributor)
- [Milos Krstajic](https://github.com/milosk) (Contributor)
- [Z. Chen](https://github.com/zhdchen) (Contributor)
- [Basil Komboz](https://github.com/bkomboz) (Contributor)

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
| [Bootstrap CSS](http://getbootstrap.com) | MIT |
| [Scalaz](https://github.com/scalaz/scalaz) | Custom |
