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
package org.hyperledger.besu.cli.options;

import static com.google.common.base.Preconditions.checkState;
import static java.util.stream.Collectors.toUnmodifiableSet;
import static org.hyperledger.besu.cli.DefaultCommandValues.MANDATORY_HOST_FORMAT_HELP;
import static org.hyperledger.besu.cli.DefaultCommandValues.MANDATORY_INTEGER_FORMAT_HELP;
import static org.hyperledger.besu.cli.DefaultCommandValues.MANDATORY_PORT_FORMAT_HELP;
import static org.hyperledger.besu.metrics.BesuMetricCategory.DEFAULT_METRIC_CATEGORIES;
import static org.hyperledger.besu.metrics.MetricsProtocol.PROMETHEUS;
import static org.hyperledger.besu.metrics.prometheus.MetricsConfiguration.DEFAULT_METRICS_PORT;
import static org.hyperledger.besu.metrics.prometheus.MetricsConfiguration.DEFAULT_METRICS_PUSH_PORT;

import org.hyperledger.besu.cli.util.CommandLineUtils;
import org.hyperledger.besu.metrics.MetricCategoryRegistryImpl;
import org.hyperledger.besu.metrics.MetricsProtocol;
import org.hyperledger.besu.metrics.prometheus.MetricsConfiguration;
import org.hyperledger.besu.plugin.services.metrics.MetricCategory;

import java.util.List;
import java.util.Set;

import picocli.CommandLine;

/** Command line options for configuring metrics. */
public class MetricsOptions implements CLIOptions<MetricsConfiguration.Builder> {
  private MetricCategoryRegistryImpl metricCategoryRegistry;

  /**
   * Returns a MetricsConfiguration.Builder because fields are often overridden from other domains,
   * like using P2P settings as defaults.
   *
   * @return a newly created {@link MetricsOptions} with default values
   */
  @Override
  public MetricsConfiguration.Builder toDomainObject() {
    MetricsConfiguration.Builder builder = MetricsConfiguration.builder();
    builder
        .timersEnabled(unstableOptions.timersEnabled)
        .idleTimeout(unstableOptions.idleTimeout)
        .enabled(getMetricsEnabled())
        .host(getMetricsHost())
        .port(getMetricsPort())
        .protocol(getMetricsProtocol())
        .metricCategories(getMetricCategories())
        .pushEnabled(getMetricsPushEnabled())
        .pushHost(getMetricsPushHost())
        .pushPort(getMetricsPushPort())
        .pushInterval(getMetricsPushInterval())
        .prometheusJob(getMetricsPrometheusJob());
    return builder;
  }

  // TODO: why do we need to be able to reverse this?
  /**
   * Returns a newly created {@link MetricsOptions} reversed from the supplied MetricsConfiguration
   *
   * @param config the metrics configuration
   * @return a newly created {@link MetricsOptions} reversed from the supplied MetricsConfiguration
   */
  public static MetricsOptions fromConfiguration(final MetricsConfiguration config) {
    final MetricsOptions metricsOptions = create();
    metricsOptions.unstableOptions.timersEnabled = config.isTimersEnabled();
    metricsOptions.unstableOptions.idleTimeout = config.getIdleTimeout();
    metricsOptions.isMetricsEnabled = config.isEnabled();
    metricsOptions.metricsHost = config.getHost();
    metricsOptions.metricsPort = config.getPort();
    metricsOptions.metricsProtocol = config.getProtocol();
    metricsOptions.metricCategories =
        config.getMetricCategories().stream()
            .map(MetricCategory::getName)
            .collect(toUnmodifiableSet());
    metricsOptions.metricsPrometheusJob = config.getPrometheusJob();
    metricsOptions.isMetricsPushEnabled = config.isPushEnabled();
    metricsOptions.metricsPushHost = config.getPushHost();
    metricsOptions.metricsPushPort = config.getPushPort();
    metricsOptions.metricsPushInterval = config.getPushInterval();
    return metricsOptions;
  }

  @Override
  public List<String> getCLIOptions() {
    return CommandLineUtils.getCLIOptions(this, new MetricsOptions());
  }

  @CommandLine.Option(
      names = {"--metrics-enabled"},
      description = "Set to start the metrics exporter (default: ${DEFAULT-VALUE})")
  private Boolean isMetricsEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--metrics-protocol"},
      description =
          "Metrics protocol, one of PROMETHEUS, OPENTELEMETRY or NONE. (default: ${DEFAULT-VALUE})")
  private MetricsProtocol metricsProtocol = PROMETHEUS;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--metrics-host"},
      paramLabel = MANDATORY_HOST_FORMAT_HELP,
      description = "Host for the metrics exporter to listen on (default: ${DEFAULT-VALUE})")
  private String metricsHost = MetricsConfiguration.DEFAULT_METRICS_HOST;

  @CommandLine.Option(
      names = {"--metrics-port"},
      paramLabel = MANDATORY_PORT_FORMAT_HELP,
      description = "Port for the metrics exporter to listen on (default: ${DEFAULT-VALUE})")
  private Integer metricsPort = DEFAULT_METRICS_PORT;

  @CommandLine.Option(
      names = {"--metrics-category", "--metrics-categories"},
      paramLabel = "<category name>",
      split = ",",
      arity = "1..*",
      description =
          "Comma separated list of categories to track metrics for (default: ${DEFAULT-VALUE})")
  private Set<String> metricCategories =
      DEFAULT_METRIC_CATEGORIES.stream().map(MetricCategory::getName).collect(toUnmodifiableSet());

  @CommandLine.Option(
      names = {"--metrics-push-enabled"},
      description = "Enable the metrics push gateway integration (default: ${DEFAULT-VALUE})")
  private Boolean isMetricsPushEnabled = false;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--metrics-push-host"},
      paramLabel = MANDATORY_HOST_FORMAT_HELP,
      description = "Host of the Prometheus Push Gateway for push mode (default: ${DEFAULT-VALUE})")
  private String metricsPushHost = MetricsConfiguration.DEFAULT_METRICS_PUSH_HOST;

  @CommandLine.Option(
      names = {"--metrics-push-port"},
      paramLabel = MANDATORY_PORT_FORMAT_HELP,
      description = "Port of the Prometheus Push Gateway for push mode (default: ${DEFAULT-VALUE})")
  private Integer metricsPushPort = DEFAULT_METRICS_PUSH_PORT;

  @CommandLine.Option(
      names = {"--metrics-push-interval"},
      paramLabel = MANDATORY_INTEGER_FORMAT_HELP,
      description =
          "Interval in seconds to push metrics when in push mode (default: ${DEFAULT-VALUE})")
  private Integer metricsPushInterval = 15;

  @SuppressWarnings({"FieldCanBeFinal", "FieldMayBeFinal"}) // PicoCLI requires non-final Strings.
  @CommandLine.Option(
      names = {"--metrics-push-prometheus-job"},
      description = "Job name to use when in push mode (default: ${DEFAULT-VALUE})")
  private String metricsPrometheusJob = "besu-client";

  @CommandLine.Option(
      names = {"--slow-block-threshold"},
      paramLabel = MANDATORY_INTEGER_FORMAT_HELP,
      description =
          "Threshold in milliseconds for logging slow blocks. Negative values disable slow block logging (default: ${DEFAULT-VALUE})")
  private Long slowBlockThresholdMs = -1L;

  /**
   * Returns a newly created {@link MetricsOptions} with default values.
   *
   * @return new instance
   */
  public static MetricsOptions create() {
    return new MetricsOptions();
  }

  private MetricsOptions() {}

  /**
   * Returns whether metrics are enabled.
   *
   * @return true if metrics are enabled, otherwise false
   */
  public Boolean getMetricsEnabled() {
    return isMetricsEnabled;
  }

  /**
   * Returns the metrics protocol.
   *
   * @return the metrics protocol
   */
  public MetricsProtocol getMetricsProtocol() {
    return metricsProtocol;
  }

  /**
   * Returns the metrics host.
   *
   * @return the metrics host
   */
  public String getMetricsHost() {
    return metricsHost;
  }

  /**
   * Returns the metrics port.
   *
   * @return the metrics port
   */
  public Integer getMetricsPort() {
    return metricsPort;
  }

  /**
   * Returns the metric categories.
   *
   * @return the metric categories
   */
  public Set<MetricCategory> getMetricCategories() {
    checkState(
        metricCategoryRegistry != null, "Set metricCategoryRegistry before calling this method");

    final var list =
        metricCategories.stream().map(metricCategoryRegistry::getMetricCategory).toList();

    return Set.copyOf(list);
  }

  /**
   * Returns whether metrics push is enabled.
   *
   * @return true if metrics push is enabled, otherwise false
   */
  public Boolean getMetricsPushEnabled() {
    return isMetricsPushEnabled;
  }

  /**
   * Returns the metrics push host.
   *
   * @return the metrics push host
   */
  public String getMetricsPushHost() {
    return metricsPushHost;
  }

  /**
   * Returns the metrics push port.
   *
   * @return the metrics push port
   */
  public Integer getMetricsPushPort() {
    return metricsPushPort;
  }

  /**
   * Returns the metrics push interval.
   *
   * @return the metrics push interval
   */
  public Integer getMetricsPushInterval() {
    return metricsPushInterval;
  }

  /**
   * Returns the metrics prometheus job.
   *
   * @return the metrics prometheus job
   */
  public String getMetricsPrometheusJob() {
    return metricsPrometheusJob;
  }

  /**
   * Returns the slow block threshold in milliseconds.
   *
   * @return the slow block threshold in milliseconds. Negative means disabled, zero logs all
   *     blocks.
   */
  public Long getSlowBlockThresholdMs() {
    return slowBlockThresholdMs;
  }

  /**
   * Perform final validation after all the options, and the metric category registry, have been set
   *
   * @param commandLine the command line
   */
  public void validate(final CommandLine commandLine) {
    checkState(
        metricCategoryRegistry != null, "Set metricCategoryRegistry before calling this method");
    final var unknownCategories =
        metricCategories.stream()
            .filter(category -> !metricCategoryRegistry.containsMetricCategory(category))
            .toList();
    if (!unknownCategories.isEmpty()) {
      throw new CommandLine.ParameterException(
          commandLine, "--metrics-categories contains unknown categories: " + unknownCategories);
    }
  }

  /**
   * Set the metric category registry in order to support verification and conversion
   *
   * @param metricCategoryRegistry the metric category registry
   */
  public void setMetricCategoryRegistry(final MetricCategoryRegistryImpl metricCategoryRegistry) {
    this.metricCategoryRegistry = metricCategoryRegistry;
  }

  @CommandLine.ArgGroup(validate = false)
  private final MetricsOptions.Unstable unstableOptions = new MetricsOptions.Unstable();

  static class Unstable {
    private static final String TIMERS_ENABLED_FLAG = "--Xmetrics-timers-enabled";
    private static final String IDLE_TIMEOUT_FLAG = "--Xmetrics-idle-timeout";

    @CommandLine.Option(
        names = TIMERS_ENABLED_FLAG,
        hidden = true,
        description = "Whether to enable timer metrics (default: ${DEFAULT-VALUE}).")
    private Boolean timersEnabled = MetricsConfiguration.DEFAULT_METRICS_TIMERS_ENABLED;

    @CommandLine.Option(
        hidden = true,
        names = {IDLE_TIMEOUT_FLAG},
        paramLabel = "<INTEGER>",
        description = "Timeout for metrics TCP connections, in seconds (default: ${DEFAULT-VALUE})")
    private int idleTimeout = MetricsConfiguration.DEFAULT_METRICS_IDLE_TIMEOUT_SECONDS;
  }
}
