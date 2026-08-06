/*
 * Copyright contributors to Besu.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * SPDX-License-Identifier: Apache-2.0
 */
package org.hyperledger.besu.ethereum.vm.operations;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Collectors;

import org.openjdk.jmh.infra.BenchmarkParams;
import org.openjdk.jmh.infra.IterationParams;
import org.openjdk.jmh.results.BenchmarkResult;
import org.openjdk.jmh.results.IterationResult;
import org.openjdk.jmh.results.RunResult;
import org.openjdk.jmh.runner.Runner;
import org.openjdk.jmh.runner.options.Options;
import org.openjdk.jmh.runner.options.OptionsBuilder;
import org.openjdk.jmh.runner.options.VerboseMode;
import org.openjdk.jmh.util.Statistics;

public final class JmhBenchmarkRunner {

  private JmhBenchmarkRunner() {}

  public static void main(final String[] args) throws Exception {
    final String include =
        args.length > 0 && !args[0].isEmpty()
            ? args[0]
            : CallDataCopyOperationBenchmark.class.getSimpleName();
    final Path output = Path.of(args.length > 1 ? args[1] : "benchmark-results.json");

    final Options opts =
        new OptionsBuilder()
            .include(".*" + include + ".*")
            .forks(3)
            .verbosity(VerboseMode.NORMAL)
            .build();

    final Collection<RunResult> results = new Runner(opts).run();
    final String json = render(results);
    Files.writeString(output, json);
    System.out.println("Wrote " + results.size() + " benchmark results to " + output);
  }

  private static String render(final Collection<RunResult> results) {
    final StringBuilder out = new StringBuilder();
    out.append("{\n");
    appendRunMetadata(out);
    out.append(",\n  \"results\": [\n");

    final List<String> rendered =
        results.stream().map(JmhBenchmarkRunner::renderResult).collect(Collectors.toList());
    for (int i = 0; i < rendered.size(); i++) {
      out.append(rendered.get(i));
      if (i < rendered.size() - 1) out.append(",");
      out.append("\n");
    }
    out.append("  ]\n}\n");
    return out.toString();
  }

  private static void appendRunMetadata(final StringBuilder out) {
    out.append("  \"run_metadata\": {\n");
    out.append("    \"date\": \"").append(Instant.now()).append("\",\n");
    out.append("    \"os\": \"").append(escape(System.getProperty("os.name"))).append("\",\n");
    out.append("    \"arch\": \"").append(escape(System.getProperty("os.arch"))).append("\",\n");
    out.append("    \"java_version\": \"")
        .append(escape(System.getProperty("java.version")))
        .append("\"\n");
    out.append("  }");
  }

  private static String renderResult(final RunResult run) {
    final BenchmarkParams params = run.getParams();
    final double score = run.getPrimaryResult().getScore();
    final double error = run.getPrimaryResult().getScoreError();
    final OptionalLong gas = GasFormulas.compute(params);
    final Double mgasPerSec =
        gas.isPresent() && score > 0 ? (1e9 / score * gas.getAsLong()) / 1e6 : null;

    final Statistics stats = run.getPrimaryResult().getStatistics();

    final StringBuilder out = new StringBuilder();
    out.append("    {\n");
    out.append("      \"benchmark\": \"").append(escape(params.getBenchmark())).append("\",\n");
    out.append("      \"params\": ").append(renderParams(params)).append(",\n");
    out.append("      \"benchmark_config\": ").append(renderBenchmarkConfig(params)).append(",\n");
    out.append("      \"ns_per_op\": ").append(score).append(",\n");
    out.append("      \"error\": ").append(error).append(",\n");
    out.append("      \"gas\": ")
        .append(gas.isPresent() ? Long.toString(gas.getAsLong()) : "null")
        .append(",\n");
    out.append("      \"mgas_per_s\": ")
        .append(mgasPerSec == null ? "null" : mgasPerSec)
        .append(",\n");
    out.append("      \"percentiles\": ").append(renderPercentiles(stats)).append(",\n");
    out.append("      \"raw_data\": ").append(renderRawData(run));
    out.append("\n    }");
    return out.toString();
  }

  private static String renderBenchmarkConfig(final BenchmarkParams params) {
    final IterationParams warmup = params.getWarmup();
    final IterationParams measurement = params.getMeasurement();
    return "{"
        + "\"forks\": "
        + params.getForks()
        + ", "
        + "\"threads\": "
        + params.getThreads()
        + ", "
        + "\"warmup_iterations\": "
        + warmup.getCount()
        + ", "
        + "\"warmup_time\": \""
        + escape(warmup.getTime().toString())
        + "\", "
        + "\"measurement_iterations\": "
        + measurement.getCount()
        + ", "
        + "\"measurement_time\": \""
        + escape(measurement.getTime().toString())
        + "\", "
        + "\"mode\": \""
        + escape(params.getMode().shortLabel())
        + "\""
        + "}";
  }

  private static String renderPercentiles(final Statistics stats) {
    return "{"
        + "\"p0\": "
        + stats.getPercentile(0.0)
        + ", "
        + "\"p25\": "
        + stats.getPercentile(25.0)
        + ", "
        + "\"p50\": "
        + stats.getPercentile(50.0)
        + ", "
        + "\"p75\": "
        + stats.getPercentile(75.0)
        + ", "
        + "\"p100\": "
        + stats.getPercentile(100.0)
        + "}";
  }

  private static String renderParams(final BenchmarkParams params) {
    final Collection<String> keys = params.getParamsKeys();
    if (keys.isEmpty()) return "{}";
    return keys.stream()
        .map(k -> "\"" + escape(k) + "\": \"" + escape(params.getParam(k)) + "\"")
        .collect(Collectors.joining(", ", "{", "}"));
  }

  private static String renderRawData(final RunResult run) {
    final List<String> perFork = new ArrayList<>();
    for (BenchmarkResult fork : run.getBenchmarkResults()) {
      final String perIter =
          fork.getIterationResults().stream()
              .map(IterationResult::getPrimaryResult)
              .map(r -> Double.toString(r.getScore()))
              .collect(Collectors.joining(", ", "[", "]"));
      perFork.add(perIter);
    }
    return "[" + String.join(", ", perFork) + "]";
  }

  private static String escape(final String s) {
    if (s == null) return "";
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }
}
