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
package org.hyperledger.besu.cli.subcommands.storage;

import org.hyperledger.besu.cli.util.VersionProvider;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.fasterxml.jackson.datatype.jdk8.Jdk8Module;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.ParentCommand;

/** The revert metadata to v1 subcommand. */
@Command(
    name = "revert-metadata",
    description = "Revert database metadata to previous format",
    mixinStandardHelpOptions = true,
    versionProvider = VersionProvider.class,
    subcommands = RevertMetadataSubCommand.v2ToV1.class)
public class RevertMetadataSubCommand implements Runnable {
  private static final Logger LOG = LoggerFactory.getLogger(RevertMetadataSubCommand.class);
  private static final String METADATA_FILENAME = "DATABASE_METADATA.json";
  private static final ObjectMapper MAPPER =
      new ObjectMapper()
          .registerModule(new Jdk8Module())
          .setSerializationInclusion(JsonInclude.Include.NON_ABSENT)
          .enable(SerializationFeature.INDENT_OUTPUT);

  @SuppressWarnings("unused")
  @ParentCommand
  private StorageSubCommand parentCommand;

  @SuppressWarnings("unused")
  @CommandLine.Spec
  private CommandLine.Model.CommandSpec spec;

  /** Default Constructor. */
  public RevertMetadataSubCommand() {}

  @Override
  public void run() {
    spec.commandLine().usage(System.out);
  }

  @Command(
      name = "v2-to-v1",
      description = "Revert a database metadata v2 format to v1 format",
      mixinStandardHelpOptions = true,
      versionProvider = VersionProvider.class)
  static class v2ToV1 implements Runnable {

    @SuppressWarnings("unused")
    @CommandLine.Spec
    private CommandLine.Model.CommandSpec spec;

    @SuppressWarnings("unused")
    @ParentCommand
    private RevertMetadataSubCommand parentCommand;

    @Override
    public void run() {

      final Path dataDir = parentCommand.parentCommand.besuCommand.dataDir();

      final File dbMetadata = dataDir.resolve(METADATA_FILENAME).toFile();
      if (!dbMetadata.exists()) {
        String errMsg =
            String.format(
                "Could not find database metadata file %s, check your data dir %s",
                dbMetadata, dataDir);
        LOG.error(errMsg);
        throw new IllegalArgumentException(errMsg);
      }
      try {
        final var root = MAPPER.readTree(dbMetadata);
        if (!root.has("v2")) {
          String errMsg =
              String.format("Database metadata file %s is not in v2 format", dbMetadata);
          LOG.error(errMsg);
          throw new IllegalArgumentException(errMsg);
        }

        final var v2Obj = root.get("v2");
        if (!v2Obj.has("format")) {
          String errMsg =
              String.format(
                  "Database metadata file %s is malformed, \"format\" field not found", dbMetadata);
          LOG.error(errMsg);
          throw new IllegalArgumentException(errMsg);
        }

        final var formatField = v2Obj.get("format").asText();

        final DataStorageFormat dataStorageFormat = DataStorageFormat.valueOf(formatField);
        final int v1Version =
            switch (dataStorageFormat) {
              case FOREST -> 1;
              case BONSAI -> 2;
              case X_BONSAI_ARCHIVE ->
                  throw new IllegalArgumentException(
                      "X_BONSAI_ARCHIVE storage format cannot be reverted to v1");
              case BINTRIE ->
                  throw new IllegalArgumentException(
                      "BINTRIE storage format cannot be reverted to v1");
            };

        @JsonSerialize
        record V1(int version) {}

        MAPPER.writeValue(dbMetadata, new V1(v1Version));
        LOG.info("Successfully reverted database metadata from v2 to v1 in {}", dbMetadata);
      } catch (IOException ioe) {
        throw new RuntimeException(ioe);
      }
    }
  }
}
