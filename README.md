# Spark Adaptive File Connector

The library provides Spark Source to efficiently handle streaming from object store for files saved in dynamically changing paths, 
e.g. `s3://my_bucket/my-data/2022/06/09/`, that might be described as `s3://my_bucket/my-data/{YYYY}/{MM}/{DD}/`.

## Usage

### Dependency

The library is [available on Maven Central](https://mvnrepository.com/artifact/ai.hunters/spark-adaptive-file-connector_2.12). 
If you use SBT you can add it to your `build.sbt`:
```sbt 
libraryDependencies += "ai.hunters" %% "spark-adaptive-file-connector" % "1.0.0"
```
for Maven:
```xml 
<dependency>
    <groupId>ai.hunters</groupId>
    <artifactId>spark-adaptive-file-connector_2.12</artifactId>
    <version>1.0.0</version>
</dependency>
```
or for [other dependency manager](https://mvnrepository.com/artifact/ai.hunters/spark-adaptive-file-connector_2.12/1.0.0).

### Spark Application
This is the simplest example of using the connector in Scala application:
```scala 
val readFileJsonStream = spark.readStream
      .format("dynamic-paths-file")
      .option("fileFormat", "json")
      .schema(yourSchema)
      .load("s3://my-bucket/prefix/{YYYY}/{MM}/{DD}")
```

## Build

    sbt "clean;compile"

## Testing

    sbt "clean;test"

## Tutorial - quick run

The easiest way to start playing with this library is an integration test `DynamicPathsFileStreamSourceProviderITest`. 
It shows how to:
* pass parameters to Spark to be able to use this source connector 
* check what paths have been read and how fast
* recover from checkpoint
* see output

When you use this library in your app please remember to register 
the DynamicPathsFileStreamSourceProvider in your DataSourceRegister. You can see example what was needed for tests in 
`src/test/resources/META-INF/services/org.apache.spark.sql.sources.DataSourceRegister`.