package io.opentelemetry.contrib.influxexporter;

import com.influxdb.client.domain.WritePrecision;
import com.influxdb.client.write.Point;
import io.opentelemetry.sdk.metrics.data.DoublePointData;
import io.opentelemetry.sdk.metrics.data.GaugeData;
import io.opentelemetry.sdk.metrics.data.HistogramData;
import io.opentelemetry.sdk.metrics.data.HistogramPointData;
import io.opentelemetry.sdk.metrics.data.LongPointData;
import io.opentelemetry.sdk.metrics.data.MetricData;
import io.opentelemetry.sdk.metrics.data.SumData;
import io.opentelemetry.sdk.metrics.data.SummaryData;
import io.opentelemetry.sdk.metrics.data.SummaryPointData;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * This class contains methods to transform instances of {@link MetricData} to {@link Point} used by
 * {@link com.influxdb.client.InfluxDBClient}.
 */
public class MetricDataTransformer {

  private MetricDataTransformer(){}

  /**
   * Transforms instances of {@link MetricData} to {@link Point}. Currently supported are Double and Long Gauges,
   * Double and Long Sums as well as Histograms and Summaries.
   * @param metricDataCollection A Collection containing instances of {@link MetricData}.
   * @return A List of {@link Point} instances based on the given Metric Data.
   */
  public static List<Point> transform(Collection<MetricData> metricDataCollection) {
    List<Point> points = new ArrayList<>();
    metricDataCollection.forEach(metricData -> {
      String name = metricData.getName();
      switch (metricData.getType()) {
        case DOUBLE_GAUGE:
          points.addAll(transformDoubleGauge(name, metricData.getDoubleGaugeData()));
          return;
        case DOUBLE_SUM:
          points.addAll(transformDoubleSum(name, metricData.getDoubleSumData()));
          return;
        case LONG_GAUGE:
          points.addAll(transformLongGauge(name, metricData.getLongGaugeData()));
          return;
        case LONG_SUM:
          points.addAll(transformLongSum(name, metricData.getLongSumData()));
          return;
        case SUMMARY:
          points.addAll(transformSummary(name, metricData.getSummaryData()));
          return;
        case HISTOGRAM:
          points.addAll(transformHistogram(name, metricData.getHistogramData()));
          return;
        default:
      }
    });
    return points;
  }

  private static List<Point> transformDoubleGauge(String name, GaugeData<DoublePointData> data) {
    List<Point> points = new ArrayList<>();
    Collection<DoublePointData> pointData = data.getPoints();

    pointData.forEach(doublePointData -> {
      Long timeStamp = doublePointData.getEpochNanos();

      Point point = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("value", doublePointData.getValue());

      doublePointData.getAttributes().forEach((attributeKey, o) -> point.addTag(attributeKey.getKey(), o.toString()));

      points.add(point);
    });

    return points;
  }

  private static List<Point> transformDoubleSum(String name, SumData<DoublePointData> data) {
    List<Point> points = new ArrayList<>();
    Collection<DoublePointData> pointData = data.getPoints();

    pointData.forEach(doublePointData -> {
      Long timeStamp = doublePointData.getEpochNanos();

      Point point = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("value", doublePointData.getValue());
      doublePointData.getAttributes().forEach((attributeKey, o) -> point.addTag(attributeKey.getKey(), o.toString()));

      points.add(point);
    });

    return points;
  }

  private static List<Point> transformLongGauge(String name, GaugeData<LongPointData> data) {
    List<Point> points = new ArrayList<>();
    Collection<LongPointData> pointData = data.getPoints();

    pointData.forEach(longPointData -> {
      Long timeStamp = longPointData.getEpochNanos();

      Point point = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("value", longPointData.getValue());
      longPointData.getAttributes().forEach((attributeKey, o) -> point.addTag(attributeKey.getKey(), o.toString()));

      points.add(point);
    });

    return points;
  }

  private static List<Point> transformLongSum(String name, SumData<LongPointData>  data) {
    List<Point> points = new ArrayList<>();
    Collection<LongPointData> pointData = data.getPoints();

    pointData.forEach(longPointData -> {
      Long timeStamp = longPointData.getEpochNanos();

      Point point = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("value", longPointData.getValue());
      longPointData.getAttributes().forEach((attributeKey, o) -> {
        point.addTag(attributeKey.getKey(), o.toString());
      });

      points.add(point);
    });
    return points;
  }

  private static List<Point> transformSummary(String name, SummaryData data) {
    List<Point> points = new ArrayList<>();
    Collection<SummaryPointData> pointData = data.getPoints();

    pointData.forEach(summaryPointData -> {
      Long timeStamp = summaryPointData.getEpochNanos();

      Point summaryPoint = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("sum", summaryPointData.getSum())
          .addField("count", summaryPointData.getCount());
      summaryPointData.getAttributes().forEach((attributeKey, o) -> {
        summaryPoint.addTag(attributeKey.getKey(), o.toString());
      });

      points.add(summaryPoint);

      summaryPointData.getValues().forEach(valueAtQuantile -> {
        Point valueQuantilePoint = new Point(name)
            .time(timeStamp, WritePrecision.NS)
            .addField("value", valueAtQuantile.getValue())
            .addField("quantile", valueAtQuantile.getQuantile());

        summaryPointData.getAttributes().forEach((attributeKey, o) -> {
          valueQuantilePoint.addTag(attributeKey.getKey(), o.toString());
        });
        points.add(valueQuantilePoint);
      });
    });
    return points;
  }

  private static List<Point> transformHistogram(String name, HistogramData data) {
    List<Point> points = new ArrayList<>();
    Collection<HistogramPointData> pointData = data.getPoints();

    pointData.forEach(histogramPointData -> {
      Long timeStamp = histogramPointData.getEpochNanos();

      Point histogramPoint = new Point(name)
          .time(timeStamp, WritePrecision.NS)
          .addField("sum", histogramPointData.getSum())
          .addField("count", histogramPointData.getCount())
          .addField("min", histogramPointData.getMin())
          .addField("max", histogramPointData.getMax());
      histogramPointData.getAttributes().forEach((attributeKey, o) -> {
        histogramPoint.addTag(attributeKey.getKey(), o.toString());
      });

      points.add(histogramPoint);
    });
    return points;
  }
}
