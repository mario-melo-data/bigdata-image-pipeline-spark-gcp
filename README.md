# Distributed Image Preprocessing Pipeline on Spark and Google Cloud

A PySpark pipeline that decodes, resizes and re-serialises an image dataset into TFRecords on Google Cloud Dataproc, with a performance study of caching, partitioning and cluster scaling under a fixed 8 vCPU compute budget.

## Overview

This project builds a distributed image preprocessing pipeline and then studies how it performs under different cluster and storage conditions. The pipeline reads JPEG images from a Google Cloud Storage bucket, applies a fixed sequence of TensorFlow image operations through PySpark RDD transformations, and writes the output as TFRecord files for later machine learning use. The focus is on methodology rather than benchmark performance: the work measures how caching, partition count, cluster topology and serialisation format affect throughput on an I/O-bound workload, and reads those results against the latency and cost trade-offs that govern large-scale data pipelines.

The analysis is run inside an 8 vCPU budget set by the coursework, using university-provided Google Cloud credits, and compares two ways of spending that budget: one large node against several small nodes.

## Data

* **Source:** TensorFlow Flowers (`tf_flowers`), read from Google's public Cloud Storage sample bucket (`gs://cloud-samples-data/ai-platform/flowers_tfrec/`).
* **Size:** source dataset of 3,670 JPEG images across 5 classes; the recorded pipeline runs processed a fixed 2% sample.
* **Processing target:** each image is decoded, resized and centre-cropped to 192x192x3, recompressed, and written into TFRecord shards. The class label is parsed from the directory name.

The raw dataset is not included in this repository. All recorded pipeline runs, locally and on every cluster, processed a 2% random sample (64 to 67 images, seed 42), as the coursework specified. The separate speed tests read the public JPEGs and Google's pre-built public TFRecord files, and measure input-format read throughput rather than the throughput of the preprocessing pipeline.

## Approach

1. **Distributed preprocessing.** TensorFlow decode, resize/crop and recompress functions are rewritten to take a single argument so they run as `RDD.map` operations, fully replacing `tf.data.Dataset` with Spark RDDs as the brief requires.
2. **TFRecord serialisation.** Each Spark partition is written as one TFRecord shard using `mapPartitionsWithIndex`, and every output filename is returned for traceability.
3. **Partition tuning.** Partition count is raised from the default 2 to 16 to spread work across more tasks.
4. **Cluster topology comparison.** The same job is run on 1-VM, 4-VM and 8-VM Dataproc clusters, all within the 8 vCPU cap, to compare horizontal and vertical scaling, using Dataproc CPU and network monitoring.
5. **RDD caching.** `cache()` is placed on the timing RDD so repeated actions are served from memory rather than re-reading from GCS.
6. **Speed test.** A grid of 32 parameter combinations is run three times each (96 measurements), comparing per-image JPEG fetches against pre-serialised TFRecord reads, with linear regression fitted to each.
7. **Theoretical discussion.** The accompanying written report discusses the results against CherryPick (Bayesian configuration search), the CAP theorem, and batch, stream and online processing scenarios.

## Results

| Experiment | Result |
|---|---|
| RDD caching | 31.09s without cache, 1.04s with cache (29.89x speedup) |
| Partition tuning (2 to 16 partitions, 2% sample) | 23.8s to 23.2s (1.03x, overhead-bound) |
| TFRecord read throughput | 6.6 to 191 images/s; 181 to 191 at the largest configuration (batch size 16, 12 batches) |
| JPEG read throughput | 2.62 to 4.45 images/s, under 5 in every run |
| Linear fit, TFRecord throughput against batch size multiplied by batch number | R-squared 0.97 |

Caching is the dominant lever: materialising the RDD in memory removes redundant network reads and changes the pipeline from network-bound to memory-bound. The topology comparison examined CPU and network activity across 1-, 4- and 8-machine clusters within the 8 vCPU budget. The patterns are consistent with an I/O-intensive workload benefiting from parallel storage access; end-to-end times were not recorded for this comparison, and it does not establish a quantified scaling advantage. At the largest speed-test configuration, TFRecord reads were about 40 times faster than per-image JPEG fetches, consistent with each JPEG read paying a separate network round-trip. TFRecord read time stayed roughly constant (0.6 to 1.5 s) from 6 to 192 images, and throughput therefore rose almost in proportion to the images read; this is what the R-squared of 0.97 reflects.

## Tech stack

Python, PySpark (RDD API), TensorFlow 2.4.0, Google Cloud Dataproc, Google Cloud Storage, gcloud CLI, Google Colab, NumPy, Matplotlib.

## Files

* `bigdata-image-pipeline-spark-gcp.ipynb` runs the full workflow: distributed preprocessing, TFRecord output, partition and cluster experiments, the caching test, the speed-test grid, and figure generation.

The notebook was developed in Google Colab, which orchestrates the pipeline and submits the distributed jobs to the Dataproc clusters. Reproducing the cloud experiments requires a Google Cloud project, Dataproc access and a Cloud Storage bucket for outputs. The project and bucket names are written into several cells and must be replaced throughout for a new environment. Input images are read from Google's public Cloud Storage bucket. Captured output logs and figures are retained in the notebook as evidence of the cloud runs.

## Reproducibility

Pipeline runs use a fixed 2% sample (seed 42), and the preprocessing logic is deterministic. The cloud throughput figures are not fully reproducible by design, because they depend on shared cloud infrastructure. The report documents this directly: JPEG runs show R-squared values between 0.08 and 0.30, reflecting network jitter, resource contention and straggler effects on a multi-tenant platform. Re-running on a different cluster or at a different time will shift the absolute numbers while preserving the relationships described above.

## Notes

This project was developed during the INM432 Big Data module of the MSc Data Science programme at City St George's, University of London. It applies PySpark RDD transformations, cloud cluster configuration, and performance analysis of I/O-bound pipelines, skills that transfer directly to data engineering and large-scale data processing in financial services, including the kind of cloud pipelines used for risk and analytics workloads.
