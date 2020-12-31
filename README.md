# Data Streaming Submission

## How did changing values on the SparkSession property parameters affect the throughput and latency of the data?

Changing the values of maxOffsetsPerTrigger and maxRatePerPartition affected maxOffsetPerSecond (latency) and processRowsPerSecond (throughput) the most in my experiments.

## What were the 2-3 most efficient SparkSession property key/value pairs? Through testing multiple variations on values, how can you tell these were the most optimal?

I tried to maximize the throughput. The following three key-value-pairs had good results:

- maxOffsetsPerTrigger: 1000
- maxRaterPerPartition: 300
- spark.default.parallelism: 100
