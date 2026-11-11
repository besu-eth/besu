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
package org.hyperledger.besu.services;

import org.hyperledger.besu.ethereum.core.plugins.PluginsVerificationMode;
import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.util.BesuVersionUtils;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.spi.LoggingEventBuilder;

/**
 * Verifies plugin integrity and compatibility for Besu.
 *
 * <p>This class performs comprehensive verification of plugins including:
 *
 * <ul>
 *   <li>Validating artifact catalogs are present
 *   <li>Checking Besu version compatibility
 *   <li>Detecting dependency conflicts between plugins
 *   <li>Detecting dependency conflicts with Besu's own dependencies
 * </ul>
 */
public class PluginVerifier {
  private static final Logger LOG = LoggerFactory.getLogger(PluginVerifier.class);

  private static final String PLUGIN_CATALOG_PATH = "META-INF/plugin-artifacts-catalog.json";
  private static final String BESU_CATALOG_PATH = "META-INF/besu-artifacts-catalog.json";

  /** I am here to make javadoc happy */
  private PluginVerifier() {}

  /**
   * Performs comprehensive verification of the detected plugins, from their classes, before any is
   * instantiated.
   *
   * <p>This method validates plugin integrity by checking:
   *
   * <ul>
   *   <li>Plugins have associated artifact catalogs
   *   <li>Plugin versions are compatible with the running Besu version
   *   <li>No dependency conflicts exist between plugins
   *   <li>No dependency conflicts exist with Besu's dependencies
   * </ul>
   *
   * @param pluginsVerificationMode the verification mode specifying which checks should fail
   * @param pluginClassLoader the class loader used to load plugins
   * @param plugins the classes of the detected plugins to verify
   * @throws IOException if an error occurs reading catalog files
   * @throws PluginVerificationException if verification fails based on the verification mode
   */
  static void verify(
      final PluginsVerificationMode pluginsVerificationMode,
      final URLClassLoader pluginClassLoader,
      final List<Class<? extends BesuPlugin>> plugins)
      throws IOException {

    final var pluginsArtifactData = getPluginsArtifactData(pluginClassLoader, plugins);
    LOG.debug("Loaded pluginsArtifactData: {}", pluginsArtifactData);

    final var catalogLessArtifacts =
        pluginsArtifactData.stream()
            .filter(ad -> ad.catalog().equals(ArtifactCatalog.NO_CATALOG))
            .toList();

    if (!catalogLessArtifacts.isEmpty()) {
      final LoggingEventBuilder leb =
          pluginsVerificationMode.failOnCatalogLess() ? LOG.atError() : LOG.atWarn();
      catalogLessArtifacts.forEach(
          ad ->
              leb.log(
                  "Artifact {} containing plugins {} is without a catalog",
                  ad.name(),
                  ad.plugins().stream().map(Class::getName).toList()));

      if (pluginsVerificationMode.failOnCatalogLess()) {
        throw new PluginVerificationException(
            "Plugins verification failed, see previous logs for more details.");
      }
    }

    verifyCatalogs(pluginClassLoader, pluginsVerificationMode, pluginsArtifactData);
  }

  /**
   * Collects artifact information for all detected plugins.
   *
   * <p>This method groups plugins by their source artifact (JAR file) and associates each artifact
   * with its metadata catalog if available.
   *
   * @param pluginClassLoader the class loader used to load plugins
   * @param plugins the classes of the detected plugins
   * @return a list of artifact information for all plugins
   * @throws IOException if an error occurs reading catalog files
   * @throws PluginVerificationException if an error occurs determining plugin artifact locations
   */
  private static List<ArtifactInfo> getPluginsArtifactData(
      final URLClassLoader pluginClassLoader, final List<Class<? extends BesuPlugin>> plugins)
      throws IOException {
    final var pluginsByArtifact =
        plugins.stream()
            .collect(
                Collectors.groupingBy(
                    pluginType -> {
                      try {
                        return pluginType
                            .getProtectionDomain()
                            .getCodeSource()
                            .getLocation()
                            .toURI()
                            .getSchemeSpecificPart();
                      } catch (URISyntaxException e) {
                        throw new PluginVerificationException(
                            "Error getting artifact URL for plugin " + pluginType.getName(), e);
                      }
                    }));

    final var allCatalogs = Collections.list(pluginClassLoader.getResources(PLUGIN_CATALOG_PATH));

    final var artifactsWithCatalogs =
        allCatalogs.stream()
            .collect(
                Collectors.toMap(
                    url -> {
                      try {
                        // JAR URLs have format jar:file:/path/to/file.jar!/entry.
                        return StringUtils.substringBefore(new URI(url.getPath()).getPath(), '!');
                      } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                      }
                    },
                    ArtifactCatalog::load));

    return pluginsByArtifact.entrySet().stream()
        .map(
            entry ->
                new ArtifactInfo(
                    entry.getKey(),
                    entry.getValue(),
                    artifactsWithCatalogs.getOrDefault(entry.getKey(), ArtifactCatalog.NO_CATALOG)))
        .toList();
  }

  /**
   * Orchestrates all catalog-based verification checks.
   *
   * @param pluginClassLoader the class loader used to load plugins
   * @param pluginsVerificationMode the verification mode specifying which checks should fail
   * @param pluginsArtifactData the artifact information for all plugins
   * @throws IOException if an error occurs reading catalog files
   * @throws PluginVerificationException if any verification check fails based on the verification
   *     mode
   */
  private static void verifyCatalogs(
      final URLClassLoader pluginClassLoader,
      final PluginsVerificationMode pluginsVerificationMode,
      final List<ArtifactInfo> pluginsArtifactData)
      throws IOException {
    verifyBesuVersions(pluginsVerificationMode, pluginsArtifactData);
    verifyBesuPluginsDependencyConflicts(
        pluginClassLoader, pluginsVerificationMode, pluginsArtifactData);
    verifyPluginsDependencyConflicts(pluginsVerificationMode, pluginsArtifactData);
  }

  /**
   * Verifies that plugins were built against a compatible Besu version.
   *
   * <p>Checks if plugins were built against the same Besu version as the currently running
   * instance. Version mismatches may indicate compatibility issues.
   *
   * @param pluginsVerificationMode the verification mode specifying whether to fail on version
   *     conflicts
   * @param pluginsArtifactData the artifact information for all plugins
   * @throws PluginVerificationException if version conflicts are found and the verification mode
   *     requires failure
   */
  private static void verifyBesuVersions(
      final PluginsVerificationMode pluginsVerificationMode,
      final List<ArtifactInfo> pluginsArtifactData) {
    final var besuRunningVersion = BesuVersionUtils.shortVersion();
    final var besuVersionConflictFound =
        pluginsArtifactData.stream()
            .anyMatch(
                artifactInfo -> {
                  final var pluginBuildBesuVersion = artifactInfo.catalog().besuVersion();
                  if (!besuRunningVersion.equals(pluginBuildBesuVersion)) {
                    final LoggingEventBuilder leb =
                        pluginsVerificationMode.failOnBesuVersionConflict()
                            ? LOG.atError()
                            : LOG.atWarn();
                    leb.log(
                        "Plugin artifact {} is built against Besu version {} while current running Besu version is {}",
                        artifactInfo.name(),
                        pluginBuildBesuVersion == null ? "unknown" : pluginBuildBesuVersion,
                        besuRunningVersion);
                    return true;
                  }
                  return false;
                });

    if (besuVersionConflictFound && pluginsVerificationMode.failOnBesuVersionConflict()) {
      throw new PluginVerificationException(
          "Plugins verification failed, see previous logs for more details.");
    }
  }

  /**
   * Verifies that there are no dependency version conflicts between plugins.
   *
   * <p>Checks if multiple plugins bring the same dependency with different versions, which could
   * lead to runtime conflicts or unexpected behavior.
   *
   * @param pluginsVerificationMode the verification mode specifying whether to fail on dependency
   *     conflicts
   * @param pluginsArtifactInfo the artifact information for all plugins
   * @throws PluginVerificationException if dependency conflicts are found and the verification mode
   *     requires failure
   */
  private static void verifyPluginsDependencyConflicts(
      final PluginsVerificationMode pluginsVerificationMode,
      final List<ArtifactInfo> pluginsArtifactInfo) {
    final var alreadyChecked = HashSet.<ArtifactInfo>newHashSet(pluginsArtifactInfo.size());

    boolean verificationFailed = false;

    for (final var artifactInfo : pluginsArtifactInfo) {
      alreadyChecked.add(artifactInfo);
      for (final var dep : artifactInfo.catalog().dependencies()) {
        for (final var otherArtifactInfo : pluginsArtifactInfo) {
          if (!alreadyChecked.contains(otherArtifactInfo)) {
            final var otherCatalog = otherArtifactInfo.catalog();
            final var maybeOtherDep = otherCatalog.getByNameAndGroup(dep);
            if (maybeOtherDep.isPresent()) {
              final var otherDep = maybeOtherDep.get();
              if (!dep.version().equals(otherDep.version())) {
                final boolean shouldFail = pluginsVerificationMode.failOnPluginDependencyConflict();
                verificationFailed = verificationFailed || shouldFail;
                final LoggingEventBuilder leb = verificationFailed ? LOG.atError() : LOG.atWarn();
                leb.log(
                    "Plugin artifacts {} and {} bring the same dependency {}:{} but with different versions: {} and {}",
                    artifactInfo.name(),
                    otherArtifactInfo.name(),
                    dep.group(),
                    dep.name(),
                    dep.version(),
                    otherDep.version());
              }
            }
          }
        }
      }
    }

    if (verificationFailed) {
      throw new PluginVerificationException(
          "Plugins verification failed, see previous logs for more details.");
    }
  }

  /**
   * Verifies that plugins do not bring dependencies already provided by Besu.
   *
   * <p>Checks if any plugin includes dependencies that are already part of Besu's classpath, which
   * could lead to class loading issues or version conflicts.
   *
   * @param pluginClassLoader the class loader used to load plugins
   * @param pluginsVerificationMode the verification mode specifying whether to fail on conflicts
   * @param pluginsArtifactData the artifact information for all plugins
   * @throws IOException if an error occurs reading the Besu catalog file
   * @throws PluginVerificationException if the Besu catalog is not found or conflicts are found and
   *     the verification mode requires failure
   */
  private static void verifyBesuPluginsDependencyConflicts(
      final URLClassLoader pluginClassLoader,
      final PluginsVerificationMode pluginsVerificationMode,
      final List<ArtifactInfo> pluginsArtifactData)
      throws IOException {
    final var besuArtifactsCatalog =
        Collections.list(pluginClassLoader.getResources(BESU_CATALOG_PATH));

    if (besuArtifactsCatalog.isEmpty()) {
      throw new PluginVerificationException("No Besu artifacts catalog found");
    }

    final var besuDependencies = ArtifactDependency.loadList(besuArtifactsCatalog.get(0));

    besuDependencies.forEach(
        besuDependency -> {
          final var conflicts =
              pluginsArtifactData.stream()
                  .filter(pad -> pad.catalog().containsByNameAndGroup(besuDependency))
                  .toList();
          if (!conflicts.isEmpty()) {
            final LoggingEventBuilder leb =
                pluginsVerificationMode.failOnBesuDependencyConflict()
                    ? LOG.atError()
                    : LOG.atWarn();
            conflicts.forEach(
                ai -> {
                  leb.log(
                      "Plugin artifact {} brings the dependency {}:{} that is already provided by Besu",
                      ai.name(),
                      besuDependency.group(),
                      besuDependency.name());
                });
            if (pluginsVerificationMode.failOnBesuDependencyConflict()) {
              throw new PluginVerificationException(
                  "Plugin verification failed due to dependency conflict, see logs for more details.");
            }
          }
        });
  }

  /**
   * Holds information about a plugin artifact (JAR file) and its associated plugins.
   *
   * @param name the file path or name of the artifact
   * @param plugins the classes of the plugins declared by this artifact
   * @param catalog the artifact's metadata catalog, or {@link ArtifactCatalog#NO_CATALOG} if none
   *     exists
   */
  private record ArtifactInfo(
      String name, List<Class<? extends BesuPlugin>> plugins, ArtifactCatalog catalog) {
    @Override
    public String toString() {
      return "ArtifactInfo{"
          + "name='"
          + name
          + '\''
          + ", plugins="
          + plugins.stream().map(Class::getName).collect(Collectors.joining(", "))
          + ", catalog="
          + catalog
          + '}';
    }
  }

  /**
   * Represents metadata about a plugin artifact including its build version and dependencies.
   *
   * @param besuVersion the version of Besu this artifact was built against, or null if unknown
   * @param dependencies the list of dependencies included in this artifact
   */
  private record ArtifactCatalog(String besuVersion, List<ArtifactDependency> dependencies) {
    /** Represents an artifact without a catalog. */
    static final ArtifactCatalog NO_CATALOG = new ArtifactCatalog(null, List.of());

    /**
     * Loads an artifact catalog from a URL.
     *
     * @param url the URL to load the catalog from
     * @return the loaded artifact catalog
     * @throws RuntimeException if an I/O error occurs
     */
    static ArtifactCatalog load(final URL url) {
      final var objectMapper = new ObjectMapper();
      try {
        return objectMapper.readValue(url, ArtifactCatalog.class);
      } catch (IOException e) {
        throw new UncheckedIOException(e);
      }
    }

    /**
     * Checks if this catalog contains a dependency matching the given dependency's name and group.
     *
     * @param other the dependency to search for
     * @return true if a matching dependency is found, false otherwise
     */
    boolean containsByNameAndGroup(final ArtifactDependency other) {
      return dependencies.stream().anyMatch(other::equalsByNameAndGroup);
    }

    /**
     * Finds a dependency in this catalog matching the given dependency's name and group.
     *
     * @param other the dependency to search for
     * @return an Optional containing the matching dependency, or empty if none found
     */
    Optional<ArtifactDependency> getByNameAndGroup(final ArtifactDependency other) {
      return dependencies.stream().filter(other::equalsByNameAndGroup).findFirst();
    }
  }

  /**
   * Represents a Maven artifact dependency with its coordinates.
   *
   * @param name the artifact name (artifactId)
   * @param group the group ID
   * @param version the version string
   * @param classifier the Maven classifier, or null if none
   * @param filename the filename of the dependency
   */
  private record ArtifactDependency(
      String name, String group, String version, String classifier, String filename) {
    /**
     * Checks if this dependency has the same name and group as another dependency.
     *
     * @param other the dependency to compare with
     * @return true if name and group match, false otherwise
     */
    boolean equalsByNameAndGroup(final ArtifactDependency other) {
      return Objects.equals(name, other.name) && Objects.equals(group, other.group);
    }

    /**
     * Loads a list of artifact dependencies from a URL.
     *
     * @param url the URL to load the dependencies from
     * @return the list of loaded artifact dependencies
     * @throws RuntimeException if an I/O error occurs
     */
    static List<ArtifactDependency> loadList(final URL url) {
      final var objectMapper = new ObjectMapper();
      try {
        return objectMapper.readValue(url, new TypeReference<>() {});
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }
  }

  /** Exception thrown when plugin verification fails. */
  private static class PluginVerificationException extends RuntimeException {
    /**
     * Constructs a new plugin verification exception with the specified message.
     *
     * @param message the detail message
     */
    PluginVerificationException(final String message) {
      super(message);
    }

    /**
     * Constructs a new plugin verification exception with the specified message and cause.
     *
     * @param message the detail message
     * @param cause the cause of the exception
     */
    PluginVerificationException(final String message, final Throwable cause) {
      super(message, cause);
    }
  }
}
