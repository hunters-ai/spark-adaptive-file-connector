# Spark Adaptive File Connector

The library provides Spark Source to efficiently handle streaming from object store for files saved in dynamically changing paths, 
e.g. `s3://my_bucket/my-data/2022/06/09/`, that might be described as `s3://my_bucket/my-data/{YYYY}/{MM}/{DD}/`.

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