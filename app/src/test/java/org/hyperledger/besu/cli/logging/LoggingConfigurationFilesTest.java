/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.cli.logging;

import static org.assertj.core.api.Assertions.assertThat;

import java.net.URI;
import java.util.stream.Stream;

import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.appender.ConsoleAppender;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.ConfigurationFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.layout.PatternLayout;
import org.apache.logging.log4j.layout.template.json.JsonTemplateLayout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

/**
 * Verifies each bundled Log4j2 configuration file loads correctly: the shared XInclude fragments
 * resolve, the Console appender uses the expected layout for its logging format, and the Splunk
 * routing appender and per-logger filters carried over from the shared fragments are present.
 */
public class LoggingConfigurationFilesTest {

  static Stream<Arguments> formats() {
    return Stream.of(
        Arguments.of("log4j2.xml", PatternLayout.class),
        Arguments.of("log4j2-ecs.xml", JsonTemplateLayout.class),
        Arguments.of("log4j2-gcp.xml", JsonTemplateLayout.class),
        Arguments.of("log4j2-logstash.xml", JsonTemplateLayout.class),
        Arguments.of("log4j2-gelf.xml", JsonTemplateLayout.class));
  }

  @ParameterizedTest
  @MethodSource("formats")
  public void bundledConfigurationLoadsWithConsoleAppenderAndExpectedLayout(
      final String resourceName, final Class<?> expectedLayoutClass) throws Exception {
    final Configuration configuration = loadConfiguration(resourceName);
    try {
      final ConsoleAppender consoleAppender =
          (ConsoleAppender) configuration.getAppenders().get("Console");
      assertThat(consoleAppender).isNotNull();
      assertThat(consoleAppender.getLayout()).isInstanceOf(expectedLayoutClass);

      // Verify the shared XInclude fragments resolved correctly.
      assertThat(configuration.getAppenders()).containsKey("Router");
      assertThat(configuration.getLoggerConfig("org.hyperledger.besu.ethereum.eth.transactions"))
          .isNotNull();
      assertThat(configuration.getRootLogger().getAppenderRefs()).hasSize(2);
    } finally {
      configuration.stop();
    }
  }

  private Configuration loadConfiguration(final String resourceName) throws Exception {
    final URI uri = getClass().getClassLoader().getResource(resourceName).toURI();
    final ConfigurationSource source = ConfigurationSource.fromUri(uri);
    final LoggerContext loggerContext = new LoggerContext("test-" + resourceName);
    final Configuration configuration =
        ConfigurationFactory.getInstance().getConfiguration(loggerContext, source);
    configuration.initialize();
    configuration.start();
    return configuration;
  }
}
