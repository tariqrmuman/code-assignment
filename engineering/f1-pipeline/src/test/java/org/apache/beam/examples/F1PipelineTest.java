package org.apache.beam.examples;

import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.testing.PAssert;
import org.apache.beam.sdk.testing.TestPipeline;
import org.apache.beam.sdk.transforms.SerializableFunction;
import org.apache.beam.sdk.values.PCollection;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;


public class F1PipelineTest {

  private static final Logger LOG = LoggerFactory.getLogger(F1PipelineTest.class);
  @Rule
  public final TestPipeline testPipeline = TestPipeline.create();

  private void runAndAssertPipeline(String fileName, String[] expected) {

    PCollection<String> input = testPipeline.apply("Read CSV",
      TextIO.read().from(getClass().getClassLoader().getResource("csv/" + fileName).getPath()));
    PCollection<String> output = F1Pipeline.runF1Pipeline(input);

    PAssert.that(output)
      .satisfies(new AssertInOrder(expected));

    testPipeline.run().waitUntilFinish();
  }

  @Test
  public void testBasicScenario() {

    String inputCsv = "basic_test.csv";
    final String[] expected = new String[]{"{\"driver\":\"DriverD\", \"average_time\":92.00, \"fastest_time\":92.0}",
      "{\"driver\":\"DriverB\", \"average_time\":95.00, \"fastest_time\":95.0}",
      "{\"driver\":\"DriverA\", \"average_time\":100.00, \"fastest_time\":100.0}"};

    runAndAssertPipeline(inputCsv, expected);
  }

  @Test
  public void testEmptyInput() {
    String inputCsv = "empty_file.csv";
    final String[] expected = new String[]{};

    runAndAssertPipeline(inputCsv, expected);
  }

  @Test
  public void testAverageScenario() {
    String inputCsv = "average_test.csv";
    final String[] expected = new String[]{"{\"driver\":\"DriverD\", \"average_time\":1.39, \"fastest_time\":1.3}",
      "{\"driver\":\"DriverA\", \"average_time\":1.40, \"fastest_time\":1.39}",
      "{\"driver\":\"DriverB\", \"average_time\":1.41, \"fastest_time\":1.38}"};

    runAndAssertPipeline(inputCsv, expected);
  }

  @Test
  public void testMissingDriverValues() {
    String inputCsv = "missing_driver_value_test.csv"; // Missing driver name
    final String[] expected = new String[]{"{\"driver\":\"DriverB\", \"average_time\":95.00, \"fastest_time\":95.0}",
      "{\"driver\":\"DriverA\", \"average_time\":100.00, \"fastest_time\":100.0}"};

    runAndAssertPipeline(inputCsv, expected);
  }

  @Test
  public void testMissingLaptimeValues() {
    String inputCsv = "missing_laptime_value_test.csv"; // Missing driver name
    final String[] expected = new String[]{"{\"driver\":\"DriverC\", \"average_time\":95.00, \"fastest_time\":95.0}",
      "{\"driver\":\"DriverA\", \"average_time\":100.00, \"fastest_time\":100.0}"};

    runAndAssertPipeline(inputCsv, expected);
  }

  @Test
  public void testLapTimeEdgeCases() {
    String inputCsv = "laptime_edge_cases_test.csv";
    final String[] expected = new String[]{"{\"driver\":\"DriverC\", \"average_time\":95.00, \"fastest_time\":95.0}"};

    runAndAssertPipeline(inputCsv, expected);
  }

  static class AssertInOrder implements SerializableFunction<Iterable<String>, Void> {
    final String[] expectedData;

    AssertInOrder(String[] expectedData) {
      this.expectedData = expectedData;
    }

    @Override
    public Void apply(Iterable<String> actualData) {
      List<String> actualList = new ArrayList<>();
      int index = 0;

      for (String s : actualData) {
        // Compare each element individually
        LOG.info("actualList:" + s);
        LOG.info("expectedData:" + expectedData[index]);
        assertThat(s, equalTo(expectedData[index]));
        index++;
      }
      return null;
    }
  }
}
