package io.opentelemetry.contrib.influxexporter;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApi;
import com.influxdb.client.WriteOptions;
import com.influxdb.client.write.Point;
import io.opentelemetry.sdk.common.CompletableResultCode;
import io.opentelemetry.sdk.metrics.InstrumentType;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.export.AggregationTemporalitySelector;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import org.jetbrains.annotations.NotNull;

import javax.annotation.concurrent.ThreadSafe;

import java.util.Collection;
import java.util.List;

/**
 * This class resembles an InfluxDB Exporter for OpenTelemetry. It is based on the {@link InfluxDBClient}.
 * To instantiate this class, use {@link InfluxExporterBuilder}.
 */
@ThreadSafe
public final class InfluxExporter implements MetricExporter {

    private static final AggregationTemporalitySelector DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR =
      AggregationTemporalitySelector.deltaPreferred();

    public static InfluxExporterBuilder builder() {
        return new InfluxExporterBuilder();
    }

    private final InfluxDBClient delegate;

    private final WriteApi influxApi;

    InfluxExporter(InfluxDBClient client, WriteOptions writeOptions) {
      this.delegate = client;
      this.influxApi = client.makeWriteApi(writeOptions);
    }

  @Override
    public AggregationTemporality getAggregationTemporality(@NotNull InstrumentType instrumentType) {
        return DEFAULT_AGGREGATION_TEMPORALITY_SELECTOR.getAggregationTemporality(instrumentType);
    }

    /**
     * Submits all the given metrics as multiple data points to the InfluxDB.
     *
     * @param metrics the list of sampled Metrics to be exported.
     *
     * @return Returns {@link CompletableResultCode#FAILURE}  if an exception occurred during exporting.
     */
    @Override
    public CompletableResultCode export(@NotNull Collection<MetricData> metrics) {
        List<Point> points = MetricDataTransformer.transform(metrics);
        try {
          influxApi.writePoints(points);
        }
        catch (RuntimeException e) {
          return CompletableResultCode.ofSuccess();
        }
        return CompletableResultCode.ofSuccess();
    }

    /**
     * Flushes the InfluxExporter WriteApi.
     *
     * @return Returns {@link CompletableResultCode#FAILURE}  if an exception occurred during flushing.
     */
    @Override
    public CompletableResultCode flush() {
      try {
        influxApi.flush();
      }
      catch (RuntimeException e) {
        return CompletableResultCode.ofFailure();
      }
      return CompletableResultCode.ofSuccess();
    }

    /** Shutdown the exporter. */
    @Override
    public CompletableResultCode shutdown() {
        delegate.close();
        influxApi.close();
        return CompletableResultCode.ofSuccess();
    }
}
