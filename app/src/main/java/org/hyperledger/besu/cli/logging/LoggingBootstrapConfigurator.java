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

import org.hyperledger.besu.cli.config.InternalProfileName;
import org.hyperledger.besu.cli.options.LoggingFormat;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.annotations.VisibleForTesting;
import org.apache.tuweni.toml.Toml;
import org.apache.tuweni.toml.TomlParseResult;

/**
 * Resolves the effective {@code --logging-format} and color settings before the first Log4j2 logger
 * is created, so that the correct bundled configuration file is loaded from the very first log
 * line. This runs ahead of Picocli parsing, so values are resolved via a best-effort scan of raw
 * CLI args, environment variables, and (if present) the {@code --config-file}/{@code --profile}
 * TOML sources, in the same precedence order as {@code ConfigDefaultValueProviderStrategy}'s
 * cascading default providers. The authoritative, fully-validated values are still established
 * later by normal Picocli parsing; this bootstrap step only needs to agree with that later
 * resolution in the common cases.
 */
public final class LoggingBootstrapConfigurator {

  static final String LOGGING_FORMAT_OPTION = "--logging-format";
  static final String LOGGING_FORMAT_ENV = "BESU_LOGGING_FORMAT";
  static final String COLOR_ENABLED_OPTION = "--color-enabled";
  static final String COLOR_ENABLED_ENV = "BESU_COLOR_ENABLED";
  static final String CONFIG_FILE_OPTION = "--config-file";
  static final String CONFIG_FILE_ENV = "BESU_CONFIG_FILE";
  static final String PROFILE_OPTION = "--profile";
  static final String PROFILE_ENV = "BESU_PROFILE";
  static final String NO_COLOR_ENV = "NO_COLOR";
  static final String LOGGING_FORMAT_TOML_KEY = "logging-format";
  static final String COLOR_ENABLED_TOML_KEY = "color-enabled";
  static final String LOG4J_CONFIGURATION_FILE_ENV = "LOG4J_CONFIGURATION_FILE";

  /** The system property Log4j2 reads to locate its configuration file. */
  static final String LOG4J_CONFIGURATION_FILE_PROPERTY = "log4j.configurationFile";

  /** The system property the declarative PLAIN console layout reads to disable ANSI colors. */
  static final String DISABLE_ANSI_PROPERTY = "besu.disableAnsi";

  private LoggingBootstrapConfigurator() {}

  /**
   * Resolves the logging format and color settings from the given CLI args and applies them as
   * system properties, before any Log4j2 logger is created.
   *
   * @param args the raw command line arguments
   */
  public static void configure(final String[] args) {
    configure(args, System.getenv());
  }

  /**
   * Same as {@link #configure(String[])} but with an injectable environment, for testing.
   *
   * @param args the raw command line arguments
   * @param environment the environment variables
   */
  @VisibleForTesting
  static void configure(final String[] args, final Map<String, String> environment) {
    configureLoggingFormat(args, environment);
    configureColor(args, environment);
  }

  private static void configureLoggingFormat(
      final String[] args, final Map<String, String> environment) {
    if (customLog4jConfigFilePresent(environment)) {
      // A user-supplied Log4j2 configuration file always takes precedence.
      return;
    }
    final LoggingFormat format = resolveLoggingFormat(args, environment);
    if (format != LoggingFormat.PLAIN) {
      System.setProperty(
          LOG4J_CONFIGURATION_FILE_PROPERTY, "classpath:" + format.getConfigResourceName());
    }
  }

  @VisibleForTesting
  static LoggingFormat resolveLoggingFormat(
      final String[] args, final Map<String, String> environment) {
    final Optional<String> value =
        scanArgValue(args, LOGGING_FORMAT_OPTION)
            .or(() -> Optional.ofNullable(environment.get(LOGGING_FORMAT_ENV)))
            .or(() -> readTomlValue(args, environment, LOGGING_FORMAT_TOML_KEY));
    if (value.isEmpty()) {
      return LoggingFormat.PLAIN;
    }
    try {
      return LoggingFormat.valueOf(value.get().toUpperCase(Locale.ROOT));
    } catch (final IllegalArgumentException e) {
      // Let the normal Picocli parsing surface a proper error for an invalid value.
      return LoggingFormat.PLAIN;
    }
  }

  private static void configureColor(final String[] args, final Map<String, String> environment) {
    System.setProperty(
        DISABLE_ANSI_PROPERTY, String.valueOf(!resolveColorEnabled(args, environment)));
  }

  @VisibleForTesting
  static boolean resolveColorEnabled(final String[] args, final Map<String, String> environment) {
    final Optional<Boolean> explicitColorEnabled =
        scanFlagValue(args, COLOR_ENABLED_OPTION)
            .or(
                () ->
                    Optional.ofNullable(environment.get(COLOR_ENABLED_ENV))
                        .map(Boolean::parseBoolean))
            .or(
                () ->
                    readTomlValue(args, environment, COLOR_ENABLED_TOML_KEY)
                        .map(Boolean::parseBoolean));
    return explicitColorEnabled.orElse(!environment.containsKey(NO_COLOR_ENV));
  }

  private static boolean customLog4jConfigFilePresent(final Map<String, String> environment) {
    return Stream.of(
            environment.get(LOG4J_CONFIGURATION_FILE_ENV),
            environment.get(LOG4J_CONFIGURATION_FILE_PROPERTY),
            System.getProperty(LOG4J_CONFIGURATION_FILE_ENV),
            System.getProperty(LOG4J_CONFIGURATION_FILE_PROPERTY))
        .flatMap(Stream::ofNullable)
        .findFirst()
        .isPresent();
  }

  // Picocli option *names* are case-sensitive (e.g. --Logging-Format is an unknown option), so
  // matching here must be too. Only the enum/boolean *value* that follows is case-insensitive,
  // matching Picocli's own setCaseInsensitiveEnumValuesAllowed(true).
  private static Optional<String> scanArgValue(final String[] args, final String optionName) {
    final String prefix = optionName + "=";
    for (int i = 0; i < args.length; i++) {
      final String arg = args[i];
      if (arg.startsWith(prefix)) {
        return Optional.of(arg.substring(prefix.length()));
      }
      if (arg.equals(optionName) && i + 1 < args.length) {
        return Optional.of(args[i + 1]);
      }
    }
    return Optional.empty();
  }

  private static Optional<Boolean> scanFlagValue(final String[] args, final String optionName) {
    final String prefix = optionName + "=";
    for (final String arg : args) {
      if (arg.startsWith(prefix)) {
        return Optional.of(Boolean.parseBoolean(arg.substring(prefix.length())));
      }
      if (arg.equals(optionName)) {
        return Optional.of(true);
      }
    }
    return Optional.empty();
  }

  // Mirrors ConfigDefaultValueProviderStrategy's CascadingDefaultProvider precedence for a value
  // not set directly on the CLI/env: --config-file, then --profile (only used as a last resort by
  // Picocli's own resolution, so it belongs after the config file here too).
  private static Optional<String> readTomlValue(
      final String[] args, final Map<String, String> environment, final String tomlKey) {
    return readConfigFileTomlValue(args, environment, tomlKey)
        .or(() -> readProfileTomlValue(args, environment, tomlKey));
  }

  private static Optional<String> readConfigFileTomlValue(
      final String[] args, final Map<String, String> environment, final String tomlKey) {
    final Optional<String> configFilePath =
        scanArgValue(args, CONFIG_FILE_OPTION)
            .or(() -> Optional.ofNullable(environment.get(CONFIG_FILE_ENV)));
    if (configFilePath.isEmpty()) {
      return Optional.empty();
    }
    final Path path = Path.of(configFilePath.get());
    if (!Files.isRegularFile(path)) {
      return Optional.empty();
    }
    try (InputStream in = Files.newInputStream(path)) {
      return extractTomlValue(Toml.parse(in), tomlKey);
    } catch (final Exception e) {
      // Best-effort only; normal Picocli/TOML parsing will surface real errors later.
      return Optional.empty();
    }
  }

  private static Optional<String> readProfileTomlValue(
      final String[] args, final Map<String, String> environment, final String tomlKey) {
    final Optional<String> profileName =
        scanArgValue(args, PROFILE_OPTION)
            .or(() -> Optional.ofNullable(environment.get(PROFILE_ENV)));
    if (profileName.isEmpty()) {
      return Optional.empty();
    }
    try (InputStream in = openProfileStream(profileName.get())) {
      if (in == null) {
        return Optional.empty();
      }
      return extractTomlValue(Toml.parse(in), tomlKey);
    } catch (final Exception e) {
      // Best-effort only; normal Picocli/TOML parsing will surface real errors later.
      return Optional.empty();
    }
  }

  // Mirrors ProfileFinder's resolution: an internal (bundled, classpath) profile by name, else an
  // external *.toml file in the profiles directory.
  private static InputStream openProfileStream(final String profileName) throws IOException {
    final Optional<InternalProfileName> internalProfile =
        InternalProfileName.valueOfIgnoreCase(profileName);
    if (internalProfile.isPresent()) {
      return LoggingBootstrapConfigurator.class
          .getClassLoader()
          .getResourceAsStream(internalProfile.get().getConfigFile());
    }
    final String profilesDirProperty = System.getProperty("besu.profiles.dir");
    final Path profilesDir =
        profilesDirProperty != null
            ? Path.of(profilesDirProperty)
            : Path.of(System.getProperty("besu.home", "."), "profiles");
    final Path externalProfile = profilesDir.resolve(profileName + ".toml");
    return Files.isRegularFile(externalProfile) ? Files.newInputStream(externalProfile) : null;
  }

  private static Optional<String> extractTomlValue(
      final TomlParseResult result, final String tomlKey) {
    if (result.hasErrors() || !result.contains(tomlKey)) {
      return Optional.empty();
    }
    // Use the generic getter rather than getString(): a TOML key can be an unquoted boolean
    // (e.g. color-enabled=false), and getString() throws a type error for non-string values.
    return Optional.ofNullable(result.get(tomlKey)).map(Object::toString);
  }
}
