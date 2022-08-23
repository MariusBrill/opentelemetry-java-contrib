/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.influxexporter;

import static java.util.Objects.requireNonNull;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.InfluxDBClientFactory;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.Point;
import io.reactivex.rxjava3.core.BackpressureOverflowStrategy;
import io.reactivex.rxjava3.core.Scheduler;

public class InfluxExporterBuilder {
  private static final String DEFAULT_ENDPOINT = "http://localhost:8086";

  private String endpoint = DEFAULT_ENDPOINT;

  private String token = "dummy_token";

  private String org = "default";

  private String bucket = "default";

  private int batchSize = WriteOptions.DEFAULT_BATCH_SIZE;

  private int flushInterval = WriteOptions.DEFAULT_FLUSH_INTERVAL;

  private int jitterInterval = WriteOptions.DEFAULT_JITTER_INTERVAL;

  private int retryInterval = WriteOptions.DEFAULT_RETRY_INTERVAL;

  private int maxRetries = WriteOptions.DEFAULT_MAX_RETRIES;

  private int maxRetryDelay = WriteOptions.DEFAULT_MAX_RETRY_DELAY;

  private int maxRetryTime = WriteOptions.DEFAULT_MAX_RETRY_TIME;

  private int exponentialBase = WriteOptions.DEFAULT_EXPONENTIAL_BASE;

  private int bufferLimit = WriteOptions.DEFAULT_EXPONENTIAL_BASE;

  private Scheduler writeSchedule = WriteOptions.DEFAULTS.getWriteScheduler();

  private BackpressureOverflowStrategy backpressureOverflowStrategy =
      WriteOptions.DEFAULTS.getBackpressureStrategy();

  public InfluxExporterBuilder() {}

  /** The amount of {@link Point} instances that are collected in a batch. */
  public InfluxExporterBuilder batchSize(int batchSize) {
    this.batchSize = batchSize;
    return this;
  }

  /** The time to wait at most until flush operation in milliseconds. */
  public InfluxExporterBuilder flushInterval(int flushInterval) {
    this.flushInterval = flushInterval;
    return this;
  }

  /** Sets the jitter interval for the flush operation in milliseconds. */
  public InfluxExporterBuilder jitterInterval(int jitterInterval) {
    this.jitterInterval = jitterInterval;
    return this;
  }

  /**
   * If no "Retry-After" header is specified by the InfluxDB Server this value is used.
   *
   * @param retryInterval A positive decimal integer defining the milliseconds to delay a retry.
   */
  public InfluxExporterBuilder retryInterval(int retryInterval) {
    this.retryInterval = retryInterval;
    return this;
  }

  /** The number of retries until a write operation fails. */
  public InfluxExporterBuilder maxRetries(int maxRetries) {
    this.maxRetries = maxRetries;
    return this;
  }

  /** The maximum delay between each retry attempt in milliseconds. */
  public InfluxExporterBuilder maxRetryDelay(int maxRetryDelay) {
    this.maxRetryDelay = maxRetryDelay;
    return this;
  }

  /**
   * Sets the maximum retry timeout in milliseconds.
   *
   * @param maxRetryTime An integer representing the maximum retry timeout.
   */
  public InfluxExporterBuilder maxRetryTime(int maxRetryTime) {
    this.maxRetryTime = maxRetryTime;
    return this;
  }

  /** Sets the base for the exponential retry delay. */
  public InfluxExporterBuilder exponentialBase(int exponentialBase) {
    this.exponentialBase = exponentialBase;
    return this;
  }

  /** Sets the maximum number of points to be stored in the retry buffer. */
  public InfluxExporterBuilder bufferLimit(int bufferLimit) {
    this.bufferLimit = bufferLimit;
    return this;
  }

  /** Sets the scheduler used to write data points. */
  public InfluxExporterBuilder writeSchedule(Scheduler writeSchedule) {
    this.writeSchedule = writeSchedule;
    return this;
  }

  /** Sets the strategy to deal with buffer overflows. */
  public InfluxExporterBuilder backpressureOverflowStrategy(
      BackpressureOverflowStrategy backpressureOverflowStrategy) {
    this.backpressureOverflowStrategy = backpressureOverflowStrategy;
    return this;
  }

  /**
   * Sets the InfluxDB endpoint to connect to. If unset, defaults to {@value DEFAULT_ENDPOINT}. The
   * endpoint must start with either http:// or https://, and include the full HTTP path.
   */
  public InfluxExporterBuilder endpoint(String endpoint) {
    requireNonNull(endpoint, "endpoint");
    this.endpoint = endpoint;
    return this;
  }

  /** Sets the token generated in Influx. */
  public InfluxExporterBuilder token(String token) {
    requireNonNull(token, "token");
    this.token = token;
    return this;
  }

  /** Sets the org in which the target bucket are located. */
  public InfluxExporterBuilder org(String org) {
    requireNonNull(org, "org");
    this.org = org;
    return this;
  }

  /** Sets the target bucket. */
  public InfluxExporterBuilder bucket(String bucket) {
    requireNonNull(bucket, "bucket");
    this.bucket = bucket;
    return this;
  }

  /**
   * Constructs a new instance of the exporter based on the builder's values.
   *
   * @return a new exporter's instance.
   */
  public InfluxExporter build() {
    WriteOptions writeOptions =
        WriteOptions.builder()
            .batchSize(batchSize)
            .flushInterval(flushInterval)
            .jitterInterval(jitterInterval)
            .retryInterval(retryInterval)
            .maxRetries(maxRetries)
            .maxRetryDelay(maxRetryDelay)
            .maxRetryTime(maxRetryTime)
            .exponentialBase(exponentialBase)
            .bufferLimit(bufferLimit)
            .writeScheduler(writeSchedule)
            .backpressureStrategy(backpressureOverflowStrategy)
            .build();
    InfluxDBClient client =
        InfluxDBClientFactory.create(this.endpoint, token.toCharArray(), org, bucket);

    return new InfluxExporter(client, writeOptions);
  }
}
