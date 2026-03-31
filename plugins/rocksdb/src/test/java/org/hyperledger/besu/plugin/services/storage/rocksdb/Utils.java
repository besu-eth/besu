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
package org.hyperledger.besu.plugin.services.storage.rocksdb;

import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;

public class Utils {
  public static final String METADATA_FILENAME = "DATABASE_METADATA.json";

  public static void createDatabaseMetadataV1(
      final Path tempDataDir, final DataStorageFormat dataStorageFormat) throws IOException {
    createDatabaseMetadataV1(tempDataDir, dataStorageFormatToV1(dataStorageFormat));
  }

  public static void createDatabaseMetadataV1(final Path tempDataDir, final int version)
      throws IOException {
    final String content = "{\"version\":" + version + "}";
    Files.write(tempDataDir.resolve(METADATA_FILENAME), content.getBytes(Charset.defaultCharset()));
  }

  public static void createDatabaseMetadataV2(
      final Path tempDataDir, final DataStorageFormat dataStorageFormat, final int version)
      throws IOException {
    final String content =
        "{\"v2\":{\"format\":\"" + dataStorageFormat + "\",\"version\":" + version + "}}";
    Files.write(tempDataDir.resolve(METADATA_FILENAME), content.getBytes(Charset.defaultCharset()));
  }

  public static void createDatabaseMetadataRaw(final Path tempDataDir, final String content)
      throws IOException {
    Files.write(tempDataDir.resolve(METADATA_FILENAME), content.getBytes(Charset.defaultCharset()));
  }

  private static int dataStorageFormatToV1(final DataStorageFormat dataStorageFormat) {
    return switch (dataStorageFormat) {
      case FOREST -> 1;
      case BONSAI -> 2;
      case X_BONSAI_ARCHIVE -> 3;
      case BINTRIE -> 4;
    };
  }
}
