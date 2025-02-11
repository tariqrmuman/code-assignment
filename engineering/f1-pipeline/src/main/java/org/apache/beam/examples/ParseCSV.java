package org.apache.beam.examples;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;

public class ParseCSV extends DoFn<String, KV<String, Double>> {
  String headerFilter;

  public ParseCSV(String headerFilter) {
    this.headerFilter = headerFilter;
  }

  @ProcessElement
  public void processElement(ProcessContext c) {
    String row = c.element();
    if (!row.equals(headerFilter)) {
      String[] parts = row.split(",");
      if (parts.length == 2) {
        String driver = parts[0].trim();
        double time = Double.parseDouble(parts[1].trim());

        if (isValidDriverName(driver) && isValidLapTime(time)) {
          c.output(KV.of(driver, time));
        }
      }
    }
  }

  private boolean isValidDriverName(String name) {
    return name != null && !name.isEmpty() && name.matches("^[A-Za-z]+$");
  }

  private boolean isValidLapTime(double lapTime) {
    return lapTime > 0 && lapTime < 1000;
  }
}
