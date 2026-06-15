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
package org.hyperledger.besu.tests.acceptance;

import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.tests.acceptance.dsl.AcceptanceTestBase;
import org.hyperledger.besu.tests.acceptance.dsl.node.configuration.BesuNodeConfigurationBuilder;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.math.BigInteger;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Optional;
import java.util.stream.Stream;

import com.google.common.io.Resources;
import org.apache.commons.compress.archivers.tar.TarArchiveEntry;
import org.apache.commons.compress.archivers.tar.TarArchiveInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.commons.compress.utils.IOUtils;

public class AbstractPreexistingNodeTest extends AcceptanceTestBase {
  protected Path hostDataPath;

  protected static void extract(final Path path, final String destDirectory) throws IOException {
    try (final TarArchiveInputStream fin =
        new TarArchiveInputStream(
            new GzipCompressorInputStream(new FileInputStream(path.toAbsolutePath().toString())))) {
      TarArchiveEntry entry;
      while ((entry = fin.getNextEntry()) != null) {
        if (entry.isDirectory()) {
          continue;
        }
        final File curfile = new File(destDirectory, entry.getName());
        final File parent = curfile.getParentFile();
        if (!parent.exists()) {
          parent.mkdirs();
        }
        try (FileOutputStream fos = new FileOutputStream(curfile)) {
          IOUtils.copy(fin, fos);
        }
      }
    }
  }

  protected BesuNodeConfigurationBuilder configureNode(
      final BesuNodeConfigurationBuilder nodeBuilder) {
    final String genesisData = getGenesisConfiguration();
    return nodeBuilder
        .devMode(false)
        .dataPath(hostDataPath)
        .genesisConfigProvider((nodes) -> Optional.of(genesisData))
        .jsonRpcEnabled();
  }

  private String getGenesisConfiguration() {
    try {
      return Resources.toString(
          hostDataPath.resolve("genesis.json").toUri().toURL(), StandardCharsets.UTF_8);
    } catch (final IOException e) {
      throw new RuntimeException(e);
    }
  }

  protected Path copyDataDir(final URL url) {
    if (url == null) {
      throw new RuntimeException("Unable to locate resource.");
    }

    try {
      final Path tmpDir = Files.createTempDirectory("data");
      Files.delete(tmpDir);
      final Path toCopy = Paths.get(url.toURI());
      try (final Stream<Path> pathStream = Files.walk(toCopy)) {
        pathStream.forEach(source -> copy(source, tmpDir.resolve(toCopy.relativize(source))));
        tmpDir.toFile().deleteOnExit();
        return tmpDir.toAbsolutePath();
      }
    } catch (final URISyntaxException | IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void copy(final Path source, final Path dest) {
    try {
      Files.copy(source, dest, REPLACE_EXISTING);
    } catch (final Exception e) {
      throw new RuntimeException(e.getMessage(), e);
    }
  }

  public static class AccountData {
    private final String accountAddress;
    private final BigInteger block;
    private final Wei expectedBalance;

    public AccountData(final String account, final BigInteger block, final Wei expectedBalance) {
      this.accountAddress = account;
      this.block = block;
      this.expectedBalance = expectedBalance;
    }

    public String getAccountAddress() {
      return accountAddress;
    }

    public BigInteger getBlock() {
      return block;
    }

    public Wei getExpectedBalance() {
      return expectedBalance;
    }
  }
}
