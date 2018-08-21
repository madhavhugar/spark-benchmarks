### Spark Benchmarks

Generate and execute benchmarks for Wordcount, Grep and Sort:

```
spark-submit --class benchmark.BenchmarkRunner --master local[*] spark-scala-maven-project-0.0.1-SNAPSHOT-jar-with-dependencies.jar > benchmarks.log 2>&1 &
```
