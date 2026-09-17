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
package org.hyperledger.besu.cli.logging;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.cli.options.LoggingFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class LoggingBootstrapConfiguratorTest {

  @TempDir private Path tempDir;

  @AfterEach
  public void clearSystemProperties() {
    System.clearProperty(LoggingBootstrapConfigurator.LOG4J_CONFIGURATION_FILE_PROPERTY);
    System.clearProperty(LoggingBootstrapConfigurator.DISABLE_ANSI_PROPERTY);
    System.clearProperty("besu.profiles.dir");
  }

  @Test
  public void resolvesPlainByDefault() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(new String[0], Map.of());
    assertThat(format).isEqualTo(LoggingFormat.PLAIN);
  }

  @Test
  public void resolvesFormatFromCliArgWithEqualsSign() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--logging-format=GCP"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.GCP);
  }

  @Test
  public void resolvesFormatFromCliArgWithSeparateValue() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--logging-format", "ecs"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.ECS);
  }

  @Test
  public void resolvesFormatFromEnvironmentVariable() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[0], Map.of("BESU_LOGGING_FORMAT", "logstash"));
    assertThat(format).isEqualTo(LoggingFormat.LOGSTASH);
  }

  @Test
  public void resolvesFormatFromTomlConfigFile() throws IOException {
    final Path config = tempDir.resolve("config.toml");
    Files.writeString(config, "logging-format=\"GELF\"\n");

    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--config-file", config.toString()}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.GELF);
  }

  @Test
  public void cliArgTakesPrecedenceOverEnvironmentVariable() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--logging-format=ECS"}, Map.of("BESU_LOGGING_FORMAT", "GCP"));
    assertThat(format).isEqualTo(LoggingFormat.ECS);
  }

  @Test
  public void environmentVariableTakesPrecedenceOverTomlConfigFile() throws IOException {
    final Path config = tempDir.resolve("config.toml");
    Files.writeString(config, "logging-format=\"GELF\"\n");

    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--config-file", config.toString()},
            Map.of("BESU_LOGGING_FORMAT", "GCP"));
    assertThat(format).isEqualTo(LoggingFormat.GCP);
  }

  @Test
  public void invalidFormatValueFallsBackToPlain() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--logging-format=BOGUS"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.PLAIN);
  }

  @Test
  public void configureSetsConfigurationFilePropertyForNonPlainFormat() {
    LoggingBootstrapConfigurator.configure(new String[] {"--logging-format=GCP"}, Map.of());
    assertThat(System.getProperty(LoggingBootstrapConfigurator.LOG4J_CONFIGURATION_FILE_PROPERTY))
        .isEqualTo("classpath:log4j2-gcp.xml");
  }

  @Test
  public void configureDoesNotSetConfigurationFilePropertyForPlainFormat() {
    LoggingBootstrapConfigurator.configure(new String[0], Map.of());
    assertThat(System.getProperty(LoggingBootstrapConfigurator.LOG4J_CONFIGURATION_FILE_PROPERTY))
        .isNull();
  }

  @Test
  public void existingLog4jConfigurationFileEnvVarTakesPrecedenceOverLoggingFormat() {
    LoggingBootstrapConfigurator.configure(
        new String[] {"--logging-format=GCP"},
        Map.of("LOG4J_CONFIGURATION_FILE", "/some/custom/log4j2.xml"));
    assertThat(System.getProperty(LoggingBootstrapConfigurator.LOG4J_CONFIGURATION_FILE_PROPERTY))
        .isNull();
  }

  @Test
  public void colorEnabledByDefaultWhenNoColorNotSet() {
    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(new String[0], Map.of());
    assertThat(colorEnabled).isTrue();
  }

  @Test
  public void colorDisabledWhenNoColorEnvVarSet() {
    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(new String[0], Map.of("NO_COLOR", "1"));
    assertThat(colorEnabled).isFalse();
  }

  @Test
  public void explicitColorEnabledCliArgOverridesNoColorEnvVar() {
    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(
            new String[] {"--color-enabled=true"}, Map.of("NO_COLOR", "1"));
    assertThat(colorEnabled).isTrue();
  }

  @Test
  public void bareColorEnabledFlagMeansTrue() {
    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(
            new String[] {"--color-enabled"}, Map.of());
    assertThat(colorEnabled).isTrue();
  }

  @Test
  public void configureSetsDisableAnsiPropertyFromColorResolution() {
    LoggingBootstrapConfigurator.configure(new String[] {"--color-enabled=false"}, Map.of());
    assertThat(System.getProperty(LoggingBootstrapConfigurator.DISABLE_ANSI_PROPERTY))
        .isEqualTo("true");
  }

  @Test
  public void resolvesColorEnabledFromTomlUnquotedBooleanValue() throws IOException {
    // color-enabled=false is an unquoted TOML boolean, not a string - readTomlValue must not
    // use getString() (which throws a type error for non-string values) to read it.
    final Path config = tempDir.resolve("config.toml");
    Files.writeString(config, "color-enabled=false\n");

    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(
            new String[] {"--config-file", config.toString()}, Map.of());
    assertThat(colorEnabled).isFalse();
  }

  @Test
  public void optionNameMatchingIsCaseSensitive() {
    // Picocli itself rejects "--Logging-Format" as an unknown option, so the bootstrap scan must
    // not match it either - matching it would load a different Log4j2 config than the one
    // Picocli's own (failing) parse would end up using.
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--Logging-Format=GCP"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.PLAIN);
  }

  @Test
  public void colorEnabledFlagMatchingIsCaseSensitive() {
    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(
            new String[] {"--Color-Enabled=false"}, Map.of());
    assertThat(colorEnabled).isTrue();
  }

  @Test
  public void enumValueMatchingRemainsCaseInsensitive() {
    // Unlike the option name, the value that follows it is legitimately case-insensitive -
    // Picocli itself accepts "--logging-format=gcp".
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--logging-format=gcp"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.GCP);
  }

  @Test
  public void resolvesFormatFromExternalProfileToml() throws IOException {
    System.setProperty("besu.profiles.dir", tempDir.toString());
    Files.writeString(tempDir.resolve("myprofile.toml"), "logging-format=\"GELF\"\n");

    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--profile", "myprofile"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.GELF);
  }

  @Test
  public void resolvesColorEnabledFromExternalProfileToml() throws IOException {
    System.setProperty("besu.profiles.dir", tempDir.toString());
    Files.writeString(tempDir.resolve("myprofile.toml"), "color-enabled=false\n");

    final boolean colorEnabled =
        LoggingBootstrapConfigurator.resolveColorEnabled(
            new String[] {"--profile", "myprofile"}, Map.of());
    assertThat(colorEnabled).isFalse();
  }

  @Test
  public void resolvesFormatFromProfileEnvironmentVariable() throws IOException {
    System.setProperty("besu.profiles.dir", tempDir.toString());
    Files.writeString(tempDir.resolve("myprofile.toml"), "logging-format=\"ECS\"\n");

    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[0], Map.of("BESU_PROFILE", "myprofile"));
    assertThat(format).isEqualTo(LoggingFormat.ECS);
  }

  @Test
  public void configFileTakesPrecedenceOverProfile() throws IOException {
    System.setProperty("besu.profiles.dir", tempDir.toString());
    Files.writeString(tempDir.resolve("myprofile.toml"), "logging-format=\"GELF\"\n");
    final Path config = tempDir.resolve("config.toml");
    Files.writeString(config, "logging-format=\"ECS\"\n");

    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--config-file", config.toString(), "--profile", "myprofile"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.ECS);
  }

  @Test
  public void unknownProfileNameFallsBackToPlain() {
    final LoggingFormat format =
        LoggingBootstrapConfigurator.resolveLoggingFormat(
            new String[] {"--profile", "does-not-exist"}, Map.of());
    assertThat(format).isEqualTo(LoggingFormat.PLAIN);
  }
}
