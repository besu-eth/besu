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
package org.hyperledger.besu.ethereum.core;

import org.hyperledger.besu.util.BesuVersionUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import jakarta.validation.constraints.NotNull;
import org.apache.maven.artifact.versioning.ComparableVersion;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class VersionMetadata implements Comparable<VersionMetadata> {
  private static final Logger LOG = LoggerFactory.getLogger(VersionMetadata.class);

  private static final String METADATA_FILENAME = "VERSION_METADATA.json";
  private static final ObjectMapper MAPPER = new ObjectMapper();
  private final String besuVersion;

  /**
   * Get the version of Besu that is running.
   *
   * @return the version of Besu
   */
  public static String getRuntimeVersionString() {
    return BesuVersionUtils.shortVersion();
  }

  public static VersionMetadata getRuntimeVersion() {
    return new VersionMetadata(getRuntimeVersionString());
  }

  @JsonCreator
  public VersionMetadata(@JsonProperty("besuVersion") final String besuVersion) {
    this.besuVersion = besuVersion;
  }

  public String getBesuVersion() {
    return besuVersion;
  }

  public static VersionMetadata lookUpFrom(final Path dataDir) throws IOException {
    LOG.info("Lookup version metadata file in data directory: {}", dataDir);
    return resolveVersionMetadata(getDefaultMetadataFile(dataDir));
  }

  public void writeToDirectory(final Path dataDir) throws IOException {
    MAPPER.writeValue(getDefaultMetadataFile(dataDir), this);
  }

  private static File getDefaultMetadataFile(final Path dataDir) throws IOException {
    File metaDataFile = dataDir.resolve(METADATA_FILENAME).toFile();

    // Create the data dir here if it doesn't exist yet
    if (!metaDataFile.getParentFile().exists()) {
      LOG.info("Data directory {} does not exist - creating it", dataDir);
      boolean created = metaDataFile.getParentFile().mkdirs();
      if (!created) {
        throw new IOException("Failed to create data directory: " + dataDir);
      }
    }
    if (!metaDataFile.getParentFile().canWrite()) {
      throw new IOException("Data directory is not writable: " + dataDir);
    }
    return metaDataFile;
  }

  private static VersionMetadata resolveVersionMetadata(final File metadataFile)
      throws IOException {
    VersionMetadata versionMetadata;
    try {
      versionMetadata = MAPPER.readValue(metadataFile, VersionMetadata.class);
      LOG.info("Existing version data detected. Besu version {}", versionMetadata.besuVersion);
    } catch (FileNotFoundException fnfe) {
      versionMetadata = new VersionMetadata(BesuVersionUtils.UNKNOWN);
    } catch (JsonProcessingException jpe) {
      throw new IllegalStateException(
          String.format("Invalid metadata file %s", metadataFile.getAbsolutePath()), jpe);
    }
    return versionMetadata;
  }

  /**
   * This function is designed to protect a Besu instance from being unintentionally started at a
   * version of Besu that might be incompatible with the version that last modified the specified
   * data directory. Currently this check is limited to checking that the version is >= the previous
   * version, to avoid accidentally running a lower version of Besu and potentially corrupting data,
   * but the method could be extended to perform any other version-to-version compatibility checks
   * necessary. If the --version-compatibility-protection flag is set to true and the compatibility
   * checks pass, the version metadata is updated to the current version of Besu.
   */
  public static void versionCompatibilityChecks(
      final boolean enforceCompatibilityProtection, final Path dataDir) throws IOException {
    final VersionMetadata metadataVersion = VersionMetadata.lookUpFrom(dataDir);
    final VersionMetadata runtimeVersion = getRuntimeVersion();
    if (metadataVersion.getBesuVersion().equals(BesuVersionUtils.UNKNOWN)) {
      // The version isn't known, potentially because the file doesn't exist. Write the latest
      // version to the metadata file.
      LOG.info(
          "No version data detected. Writing Besu version {} to metadata file",
          runtimeVersion.getBesuVersion());
      runtimeVersion.writeToDirectory(dataDir);
    } else {
      // Check the runtime version against the most recent version as recorded in the version
      // metadata file
      final int versionComparison = runtimeVersion.compareTo(metadataVersion);
      if (versionComparison == 0) {
        // Versions match - no-op
      } else if (versionComparison < 0) {
        if (!enforceCompatibilityProtection) {
          LOG.warn(
              "Besu version {} is lower than version {} that last started. Allowing startup because --version-compatibility-protection has been disabled.",
              runtimeVersion.getBesuVersion(),
              metadataVersion.getBesuVersion());
          // We've allowed startup at an older version of Besu. Since the version in the metadata
          // file records the latest version of
          // Besu to write to the database we'll update the metadata version to this
          // downgraded-version.
          runtimeVersion.writeToDirectory(dataDir);
        } else {
          final String message =
              "Besu version "
                  + runtimeVersion.getBesuVersion()
                  + " is lower than version "
                  + metadataVersion.getBesuVersion()
                  + " that last started. Remove --version-compatibility-protection option to allow Besu to start at "
                  + " the lower version (warning - this may have unrecoverable effects on the database).";
          LOG.error(message);
          throw new IllegalStateException(message);
        }
      } else {
        LOG.info(
            "Besu version {} is higher than version {} that last started. Updating version metadata.",
            runtimeVersion.getBesuVersion(),
            metadataVersion.getBesuVersion());
        runtimeVersion.writeToDirectory(dataDir);
      }
    }
  }

  @Override
  public int compareTo(@NotNull final VersionMetadata versionMetadata) {
    final String thisVersion = this.getBesuVersion().split("-", 2)[0];
    final String metadataVersion = versionMetadata.getBesuVersion().split("-", 2)[0];
    return new ComparableVersion(thisVersion).compareTo(new ComparableVersion(metadataVersion));
  }
}
