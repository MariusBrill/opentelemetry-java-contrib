/*
 * Copyright The OpenTelemetry Authors
 * SPDX-License-Identifier: Apache-2.0
 */

package io.opentelemetry.contrib.influxexporter;

import static io.opentelemetry.api.common.AttributeKey.stringKey;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.influxdb.client.InfluxDBClient;
import com.influxdb.client.WriteApiBlocking;
import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.sdk.common.InstrumentationScopeInfo;
import io.opentelemetry.sdk.metrics.data.AggregationTemporality;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableDoublePointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableGaugeData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableHistogramPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableLongPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableMetricData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSumData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableSummaryPointData;
import io.opentelemetry.sdk.metrics.internal.data.ImmutableValueAtQuantile;
import io.opentelemetry.sdk.resources.Resource;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class InfluxExporterTest {
  @Mock InfluxDBClient mockInfluxClient;

  @InjectMocks InfluxExporter influxExporter;

  @Test
  void exportDoubleGaugeData() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateDoubleGaugeData());
    influxExporter.export(testData);

    Point expectedDataPoint =
        new Point("double_gauge_test")
            .addField("value", 1.0)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking).writePoint(argument.capture());
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint.toLineProtocol(), argument.getValue().toLineProtocol());
  }

  @Test
  void exportLongGaugeData() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateLongGauge());
    influxExporter.export(testData);

    Point expectedDataPoint =
        new Point("long_gauge_test")
            .addField("value", 4L)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking).writePoint(argument.capture());
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint.toLineProtocol(), argument.getValue().toLineProtocol());
  }

  @Test
  void exportDoubleSumData() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateDoubleSumData());
    influxExporter.export(testData);

    Point expectedDataPoint =
        new Point("double_sum_test")
            .addField("value", 2.0)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking).writePoint(argument.capture());
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint.toLineProtocol(), argument.getValue().toLineProtocol());
  }

  @Test
  void exportLongSumData() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateLongSum());
    influxExporter.export(testData);

    Point expectedDataPoint =
        new Point("long_sum_test")
            .addField("value", 3L)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking).writePoint(argument.capture());
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint.toLineProtocol(), argument.getValue().toLineProtocol());
  }

  @Test
  void exportHistogram() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateHistogram());
    influxExporter.export(testData);

    Point expectedDataPoint =
        new Point("histogram_test")
            .addField("count", 10)
            .addField("max", 5.0)
            .addField("min", 5.0)
            .addField("sum", 5.0)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking, times(2)).writePoint(argument.capture());
    List<Point> data = argument.getAllValues();
    assertThat(data.size()).isEqualTo(2);
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint.toLineProtocol(), argument.getValue().toLineProtocol());
  }

  @Test
  void exportSummary() {
    ArgumentCaptor<Point> argument = ArgumentCaptor.forClass(Point.class);
    WriteApiBlocking mockWriteApiBlocking = Mockito.mock(WriteApiBlocking.class);
    when(mockInfluxClient.getWriteApiBlocking()).thenReturn(mockWriteApiBlocking);

    List<MetricData> testData = Collections.singletonList(generateSummary());
    influxExporter.export(testData);

    Point expectedDataPoint1 =
        new Point("summary_test")
            .addField("count", 6)
            .addField("sum", 6.0)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    Point expectedDataPoint2 =
        new Point("summary_test")
            .addField("quantile", 6.0)
            .addField("value", 6.0)
            .addTag("k", "v")
            .time(200L, WritePrecision.NS);
    verify(mockWriteApiBlocking, times(2)).writePoint(argument.capture());
    List<Point> data = argument.getAllValues();
    assertEquals(data.size(), 2);
    // We compare the LineProtocols here because Point is a read only class and the LineProtocol
    // contains all parameters.
    assertEquals(expectedDataPoint1.toLineProtocol(), data.get(0).toLineProtocol());
    assertEquals(expectedDataPoint2.toLineProtocol(), data.get(1).toLineProtocol());
  }

  static MetricData generateDoubleGaugeData() {
    return ImmutableMetricData.createDoubleGauge(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "double_gauge_test",
        "description",
        "1",
        ImmutableGaugeData.create(
            Collections.singletonList(
                ImmutableDoublePointData.create(1L, 200L, Attributes.of(stringKey("k"), "v"), 1))));
  }

  static MetricData generateDoubleSumData() {
    ImmutableDoublePointData data =
        (ImmutableDoublePointData)
            ImmutableDoublePointData.create(1L, 200L, Attributes.of(stringKey("k"), "v"), 2);
    List<DoublePointData> dataList = Collections.singletonList(data);
    ImmutableSumData<DoublePointData> sumData =
        ImmutableSumData.create(
            /* isMonotonic= */ true, AggregationTemporality.CUMULATIVE, dataList);
    return ImmutableMetricData.createDoubleSum(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "double_sum_test",
        "description",
        "1",
        sumData);
  }

  static MetricData generateLongSum() {
    return ImmutableMetricData.createLongSum(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "long_sum_test",
        "description",
        "1",
        ImmutableSumData.create(
            /* isMonotonic= */ true,
            AggregationTemporality.CUMULATIVE,
            Collections.singletonList(
                ImmutableLongPointData.create(1L, 200L, Attributes.of(stringKey("k"), "v"), 3))));
  }

  static MetricData generateLongGauge() {
    return ImmutableMetricData.createLongGauge(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "long_gauge_test",
        "description",
        "1",
        ImmutableGaugeData.create(
            Collections.singletonList(
                ImmutableLongPointData.create(1L, 200L, Attributes.of(stringKey("k"), "v"), 4))));
  }

  static MetricData generateHistogram() {
    List<Double> boundaries = Collections.singletonList(5.0);
    List<Long> counts = new ArrayList<>();
    counts.add(5L);
    counts.add(5L);
    List<HistogramPointData> pointDataList = new ArrayList<>();

    ImmutableHistogramPointData pointData =
        ImmutableHistogramPointData.create(
            1L, 200L, Attributes.of(stringKey("k"), "v"), 5.0, 5.0, 5.0, boundaries, counts);
    pointDataList.add(pointData);
    pointDataList.add(pointData);

    return ImmutableMetricData.createDoubleHistogram(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "histogram_test",
        "description",
        "1",
        ImmutableHistogramData.create(AggregationTemporality.CUMULATIVE, pointDataList));
  }

  static MetricData generateSummary() {
    return ImmutableMetricData.createDoubleSummary(
        Resource.empty(),
        InstrumentationScopeInfo.empty(),
        "summary_test",
        "description",
        "1",
        ImmutableSummaryData.create(
            Collections.singletonList(
                ImmutableSummaryPointData.create(
                    1L,
                    200L,
                    Attributes.of(stringKey("k"), "v"),
                    6,
                    6,
                    Collections.singletonList(ImmutableValueAtQuantile.create(6.0, 6.0))))));
  }
}
