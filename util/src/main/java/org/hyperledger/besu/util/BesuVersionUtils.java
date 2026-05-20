/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.util;

import org.hyperledger.besu.util.platform.PlatformDetector;

import java.net.JarURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.Optional;
import java.util.jar.Attributes;
import java.util.jar.Manifest;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Represent Besu information such as version, OS etc. Used with --version option and during Besu
 * start.
 */
public final class BesuVersionUtils {
  private static final Logger LOG = LoggerFactory.getLogger(BesuVersionUtils.class);

  /** Sentinel value used when the version or commit metadata is not available. */
  public static final String UNKNOWN = "UNKNOWN";

  private static final String CLIENT = "besu";
  private static final String VERSION;
  private static final String OS = PlatformDetector.getOS();
  private static final String VM = PlatformDetector.getVM();
  private static final String COMMIT;

  static {
    String className = BesuVersionUtils.class.getSimpleName() + ".class";
    final URL classUrl = BesuVersionUtils.class.getResource(className);

    String commit = null;
    String implVersion =
        Optional.ofNullable(BesuVersionUtils.class.getPackage())
            .map(Package::getImplementationVersion)
            .orElse(null);
    if (classUrl != null) {
      try {
        JarURLConnection jarConnection = (JarURLConnection) classUrl.openConnection();
        Manifest manifest = jarConnection.getManifest();
        Attributes attributes = manifest.getMainAttributes();
        commit = attributes.getValue("Commit-Hash");
        if (implVersion == null) {
          // workaround fallback: when running tests it could happen that the first class loaded in
          // the package is a test class and so the package is created without the manifest
          implVersion = attributes.getValue("Implementation-Version");
        }
      } catch (Exception ignored) {
        LOG.warn(
            "Partial or missing Besu version metadata, commit: {} version: {}",
            Optional.ofNullable(commit).orElse("NONE/null"),
            Optional.ofNullable(implVersion).orElse("NONE/null"));
      }
    }
    COMMIT = commit != null ? commit : UNKNOWN;
    VERSION = implVersion != null ? implVersion : UNKNOWN;
  }

  private BesuVersionUtils() {}

  /**
   * Generate version-only Besu version
   *
   * @return Besu version in format such as "v23.1.0" or "v23.1.1-dev-ac23d311", or {@value
   *     #UNKNOWN} if not available
   */
  public static String shortVersion() {
    return VERSION;
  }

  /**
   * Generate version suitable to be used in the extra data field of a block header
   *
   * @return Besu version in format such as "v23.1.0" or "v23.1.1-dev-ac23d311" as UTF-8 encoded
   *     bytes
   */
  public static Bytes versionForExtraData() {
    final var nameAndVersion = ("besu " + VERSION).getBytes(StandardCharsets.UTF_8);
    return Bytes.wrap(nameAndVersion, 0, Math.min(32, nameAndVersion.length));
  }

  /**
   * Generate full Besu version
   *
   * @return Besu version in format such as "besu/v23.1.1-dev-ac23d311/osx-x86_64/graalvm-java-17"
   *     or "besu/v23.1.0/osx-aarch_64/corretto-java-19"
   */
  public static String version() {
    return String.format("%s/v%s/%s/%s", CLIENT, VERSION, OS, VM);
  }

  /**
   * Generate node name including identity.
   *
   * @param maybeIdentity optional node identity to include in the version string.
   * @return Version with optional identity if provided.
   */
  public static String nodeName(final Optional<String> maybeIdentity) {
    return maybeIdentity
        .map(identity -> String.format("%s/%s/v%s/%s/%s", CLIENT, identity, VERSION, OS, VM))
        .orElse(version());
  }

  /**
   * Generate the commit hash for this besu version
   *
   * @return the commit hash for this besu version, or {@value #UNKNOWN} if not available
   */
  public static String commit() {
    return COMMIT;
  }
}
