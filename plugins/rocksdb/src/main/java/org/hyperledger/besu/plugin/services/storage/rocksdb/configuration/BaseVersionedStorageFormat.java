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
package org.hyperledger.besu.plugin.services.storage.rocksdb.configuration;

import org.hyperledger.besu.plugin.services.storage.DataStorageConfiguration;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

/** Base versioned data storage format */
public enum BaseVersionedStorageFormat implements VersionedStorageFormat {
  /** Original Forest version, not used since replace by FOREST_WITH_VARIABLES */
  FOREST_ORIGINAL(DataStorageFormat.FOREST, 1),
  /**
   * Current Forest version, with blockchain variables in a dedicated column family, in order to
   * make BlobDB more effective
   */
  FOREST_WITH_VARIABLES(DataStorageFormat.FOREST, 2),
  /**
   * Current Forest version, with receipts using compaction, in order to make Receipts use less disk
   * space
   */
  FOREST_WITH_RECEIPT_COMPACTION(DataStorageFormat.FOREST, 3),
  /** Original Bonsai version, not used since replace by BONSAI_WITH_VARIABLES */
  BONSAI_ORIGINAL(DataStorageFormat.BONSAI, 1),
  /**
   * Current Bonsai version, with blockchain variables in a dedicated column family, in order to
   * make BlobDB more effective
   */
  BONSAI_WITH_VARIABLES(DataStorageFormat.BONSAI, 2),
  /**
   * Current Bonsai version, with receipts using compaction, in order to make Receipts use less disk
   * space
   */
  BONSAI_WITH_RECEIPT_COMPACTION(DataStorageFormat.BONSAI, 3),
  /**
   * Current Bonsai archive version, with blockchain variables in a dedicated column family, in
   * order to make BlobDB more effective
   */
  BONSAI_ARCHIVE_WITH_VARIABLES(DataStorageFormat.X_BONSAI_ARCHIVE, 1),
  /**
   * Current Bonsai archive version, with receipts using compaction, in order to make Receipts use
   * less disk space
   */
  BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION(DataStorageFormat.X_BONSAI_ARCHIVE, 2);

  private final DataStorageFormat format;
  private final int version;

  BaseVersionedStorageFormat(final DataStorageFormat format, final int version) {
    this.format = format;
    this.version = version;
  }

  /**
   * Return the default version for new db for a specific format
   *
   * @param configuration data storage configuration
   * @return the version to use for new db
   */
  public static BaseVersionedStorageFormat defaultForNewDB(
      final DataStorageConfiguration configuration) {
    return switch (configuration.getDataStorageFormat()) {
      case FOREST -> FOREST_WITH_RECEIPT_COMPACTION;
      case BONSAI -> BONSAI_WITH_RECEIPT_COMPACTION;
      case X_BONSAI_ARCHIVE -> BONSAI_ARCHIVE_WITH_RECEIPT_COMPACTION;
    };
  }

  @Override
  public DataStorageFormat getFormat() {
    return format;
  }

  @Override
  public int getVersion() {
    return version;
  }

  @Override
  public String toString() {
    return "BaseVersionedStorageFormat{" + "format=" + format + ", version=" + version + '}';
  }
}
