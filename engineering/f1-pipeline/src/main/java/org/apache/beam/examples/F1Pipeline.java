package org.apache.beam.examples;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.metrics.Counter;
import org.apache.beam.sdk.metrics.Metrics;
import org.apache.beam.sdk.transforms.*;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.Comparator;

public class F1Pipeline {

  private static final Logger LOG = LoggerFactory.getLogger(F1Pipeline.class);
  private static final Counter validationFailures = Metrics.counter("custom", "validationFailures");

  public static void main(String[] args) {
    Pipeline pipeline = Pipeline.create();

    PCollection<String> input = pipeline.apply("Read CSV", TextIO.read().from("laptimes.csv"));

    PCollection<String> output = runF1Pipeline(input);

    output.apply(TextIO.write().to("output").withSuffix(".json").withoutSharding());

    pipeline.run();
  }

  public static PCollection<String> runF1Pipeline(PCollection<String> input) {
    String header = "Driver,Time";
    PCollection<KV<String, Double>> driverLapTimes = input
      .apply("ParseCSV", ParDo.of(new ParseCSV(header)));

    PCollection<KV<String, Double>> driverAvgLaptime = driverLapTimes.apply("CalculateAverageLapTime", Mean.perKey());
    PCollection<KV<String, Double>> driverFastestLaptime = driverLapTimes.apply("CalculateFastestLapTime", Min.perKey());

    // Merge the driverFastestLaptime and top3Drivers using CoGroupByKey
    PCollection<KV<String, CoGbkResult>> coGrouped = KeyedPCollectionTuple
      .of("avgLapTime", driverAvgLaptime)
      .and("fastestLapTime", driverFastestLaptime)
      .apply(CoGroupByKey.create());

    PCollection<KV<String, CoGbkResult>> top3Drivers = coGrouped
      .apply("GetTop3", Top.of(3, new LapTimeComparator()))
      .apply("Flatten", Flatten.iterables());

    PCollection<String> output = top3Drivers
      .apply("CombineData", ParDo.of(new DoFn<KV<String, CoGbkResult>, String>() {
        @ProcessElement
        public void processElement(ProcessContext c) {
          KV<String, CoGbkResult> element = c.element();
          String driver = element.getKey();
          Double avgTime = element.getValue().getOnly("avgLapTime", Double.NaN);
          Double fastestTime = element.getValue().getOnly("fastestLapTime", Double.NaN);

          // Format avgTime to two decimal places
          String formattedAvgTime = String.format("%.2f", avgTime);

          String json = "{\"driver\":\"" + driver + "\", \"average_time\":" + formattedAvgTime + ", \"fastest_time\":" + fastestTime + "}";
          c.output(json);
        }
      }));
    return output;
  }

  static class LapTimeComparator implements Comparator<KV<String, CoGbkResult>>, Serializable {
    @Override
    public int compare(KV<String, CoGbkResult> o1, KV<String, CoGbkResult> o2) {
      return Double.compare(o2.getValue().getOnly("avgLapTime"), o1.getValue().getOnly("avgLapTime"));
    }
  }

}
