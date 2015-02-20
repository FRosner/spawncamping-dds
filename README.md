## Data Driven Spark (spawncamping-dds)

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
    
### Licensing

This project is licensed under the Apache License Version 2.0. For details please see the file called LICENSE.
