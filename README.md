# Distributed Image Preprocessing Pipeline on Spark and Google Cloud

A PySpark pipeline that decodes, resizes and re-serialises an image dataset into TFRecords on Google Cloud Dataproc, with a performance study of caching, partitioning and cluster scaling under a fixed free-tier compute budget.

## Overview

This project builds a distributed image preprocessing pipeline and then studies how it performs under different cluster and storage conditions. The pipeline reads JPEG images from a Google Cloud Storage bucket, applies a fixed sequence of TensorFlow image operations through PySpark RDD transformations, and writes the output as TFRecord files for later machine learning use. The focus is on methodology rather than benchmark performance: the work measures how caching, partition count, cluster topology and serialisation format affect throughput on an I/O-bound workload, and reads those results against the latency and cost trade-offs that govern large-scale data pipelines.

The analysis is run inside an 8 vCPU envelope, the Google Cloud free-tier limit, and compares two ways of spending that budget: one large node against several small nodes.

## Data

* **Source:** TensorFlow Flowers (`tf_flowers`), read from a Google Cloud Storage bucket.
* **Size:** 3,670 JPEG images across 5 flower classes.
* **Processing target:** each image is decoded, resized and centre-cropped to 192x192x3, recompressed, and written into TFRecord shards. The class label is parsed from the directory name.

The raw dataset is not included in this repository. It is the public `tf_flowers` collection and must be staged in a cloud storage bucket before the distributed cells can run.

## Approach

1. **Distributed preprocessing.** TensorFlow decode, resize/crop and recompress functions are rewritten to take a single argument so they run as `RDD.map` operations, fully replacing `tf.data.Dataset` with Spark RDDs as the brief requires.
2. **TFRecord serialisation.** Each Spark partition is written as one TFRecord shard using `mapPartitionsWithIndex`, and every output filename is returned for traceability.
3. **Partition tuning.** Partition count is raised from the default 2 to 16 to spread work across more tasks.
4. **Cluster topology comparison.** The same job is run on 1-VM, 4-VM and 8-VM Dataproc clusters, all within the 8 vCPU cap, to isolate the effect of horizontal scaling on an I/O-bound workload.
5. **RDD caching.** `cache()` is placed on the timing RDD so repeated actions are served from memory rather than re-reading from GCS.
6. **Speed test.** A grid of 32 parameter combinations is run three times each (96 measurements), comparing per-image JPEG fetches against pre-serialised TFRecord reads, with linear regression fitted to each.
7. **Theoretical discussion.** Results are read against CherryPick (Bayesian configuration search), the CAP theorem, and batch, stream and online processing scenarios.

## Results

| Experiment | Result |
|---|---|
| RDD caching | 31.09s without cache, 1.04s with cache (29.89x speedup) |
| Partition tuning (2 to 16) | 23.8s to 23.2s (1.03x, overhead-bound) |
| TFRecord read throughput | over 100 images/s on average, peaks above 180 images/s |
| JPEG read throughput | 2.62 to 4.07 images/s |
| Linear fit, TFRecord throughput | R-squared 0.97 |

Caching is the dominant lever: materialising the RDD in memory removes redundant network reads and changes the pipeline from network-bound to memory-bound. Horizontal scaling helps because the workload is I/O-bound, so aggregate network bandwidth to GCS sets the ceiling rather than per-node compute. Pre-serialised TFRecord reads are roughly two orders of magnitude faster than per-image JPEG fetches, because each JPEG read pays a fresh network round-trip that cannot be amortised.

## Tech stack

Python, PySpark (RDD API), TensorFlow 2.4.0, Google Cloud Dataproc, Google Cloud Storage, gcloud CLI, NumPy, Matplotlib.

## Files

* `bigdata-image-pipeline-spark-gcp.ipynb` runs the full workflow: distributed preprocessing, TFRecord output, partition and cluster experiments, the caching test, the speed-test grid, and figure generation.

The distributed cells require a Google Cloud project, a storage bucket holding the dataset, and a Dataproc cluster. To run them, stage `tf_flowers` in a bucket, set the project and bucket variables, then run the notebook cells in order. Captured output logs and figures are retained in the notebook as evidence of the cloud runs.

## Reproducibility

Local test runs are reproducible: a 2% sample of the data is used for fast checks, and the preprocessing logic is deterministic. The cloud throughput figures are not fully reproducible by design, because they depend on shared cloud infrastructure. The report documents this directly: JPEG runs show R-squared values between 0.08 and 0.30, reflecting network jitter, resource contention and straggler effects on a multi-tenant platform. Re-running on a different cluster or at a different time will shift the absolute numbers while preserving the relationships described above.

## Notes

This project was developed during the INM432 Big Data module of the MSc Data Science programme at City St George's, University of London. It applies PySpark RDD transformations, cloud cluster configuration, and performance analysis of I/O-bound pipelines, skills that transfer directly to data engineering and large-scale data processing in financial services, including the kind of cloud pipelines used for risk and analytics workloads.
