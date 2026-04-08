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
package org.hyperledger.besu.cli.subcommands.storage;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.worldstate.DataStorageConfiguration.DEFAULT_BONSAI_CONFIG;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockchainSetupUtil;
import org.hyperledger.besu.ethereum.storage.StorageProvider;
import org.hyperledger.besu.ethereum.storage.keyvalue.WorldStatePreimageKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.BonsaiWorldStateProvider;
import org.hyperledger.besu.ethereum.worldstate.ImmutableDataStorageConfiguration;
import org.hyperledger.besu.ethereum.worldstate.WorldStatePreimageStorage;
import org.hyperledger.besu.plugin.services.storage.DataStorageFormat;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class PreimageSubCommandTest {

  DataStorageFormat preimageStorageConfiguration =
      ImmutableDataStorageConfiguration.builder()
          .from(DEFAULT_BONSAI_CONFIG)
          .hashPreImageStorageEnabled(true)
          .build()
          .getDataStorageFormat();

  @Test
  public void assertAllChainPreimagesExist() {
    var chainSetup = BlockchainSetupUtil.forTesting(preimageStorageConfiguration);
    chainSetup.importAllBlocks();
    BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) chainSetup.getWorldArchive();
    var preimageStorage = archive.getWorldStateKeyValueStorage().getPreimageStorage();

    // use block coinbase to assert a preimage we know by default
    var coinbase = chainSetup.getBlockchain().getChainHeadHeader().getCoinbase();

    var coinbasePreimage = preimageStorage.getAccountTrieKeyPreimage(Hash.hash(coinbase));
    assertThat(coinbasePreimage).isPresent();
    assertThat(coinbasePreimage.get()).isEqualTo(coinbase);

    // get all flat accounts and assert their preimages exist:
    final var ws = archive.getWorldStateKeyValueStorage();
    var acctStreamMap = ws.streamFlatAccounts(UInt256.ZERO, UInt256.MAX_VALUE, 100L);
    assertThat(acctStreamMap).isNotEmpty();
    acctStreamMap.keySet().stream()
        .map(Hash::wrap)
        .forEach(
            acctHash -> {
              var acctPreimage = preimageStorage.getAccountTrieKeyPreimage(Hash.wrap(acctHash));
              assertThat(acctPreimage).isPresent();

              // get all flat storage and assert their key preimages exist:
              var acctStorage =
                  ws.streamFlatStorages(acctHash, UInt256.ZERO, UInt256.MAX_VALUE, 100L);
              acctStorage.keySet().stream()
                  .forEach(
                      slotHash -> {
                        var slotPreimage = preimageStorage.getStorageTrieKeyPreimage(slotHash);
                        assertThat(slotPreimage).isPresent();
                      });
            });
  }

  @Test
  public void assertPreimageExport(@TempDir final Path tempDir) throws IOException {
    Path tempFile = tempDir.resolve("export_preimages.txt");
    var chainSetup = BlockchainSetupUtil.forTesting(preimageStorageConfiguration);
    BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) chainSetup.getWorldArchive();
    var preimageStorage = archive.getWorldStateKeyValueStorage().getPreimageStorage();

    chainSetup.importAllBlocks();
    var cmd = new PreimageSubCommand.Export();
    cmd.storageProvider = getMockStorageProvider(preimageStorage);
    cmd.hex = true;
    cmd.preimageFilePath = tempFile;
    cmd.run();

    // smoke check to ensure the export isn't empty:
    assertThat(Files.readAllLines(tempFile).size()).isGreaterThan(1);

    try (Stream<String> lines = Files.lines(tempFile)) {
      lines.forEach(
          line -> {
            var preimage = Bytes.fromHexString(line);
            var hashVal = Hash.hash(preimage);
            var lookupRaw = preimageStorage.getRawPreimage(hashVal);
            assertThat(lookupRaw).isPresent();
            assertThat(lookupRaw.get()).isEqualTo(preimage);
          });
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Test
  public void assertPreimageImportIsAdditive(@TempDir final Path tempDir) throws IOException {
    Path tempFile = tempDir.resolve("import_preimages.txt");
    Address mockAddress = Address.fromHexString("0xdeadbeef");
    Files.writeString(tempFile, mockAddress.toHexString() + "\n");

    var chainSetup = BlockchainSetupUtil.forTesting(preimageStorageConfiguration);
    BonsaiWorldStateProvider archive = (BonsaiWorldStateProvider) chainSetup.getWorldArchive();
    var preimageStorage = archive.getWorldStateKeyValueStorage().getPreimageStorage();
    chainSetup.importAllBlocks();

    assertThat(preimageStorage.getRawPreimage(mockAddress.addressHash())).isEmpty();

    var cmd = new PreimageSubCommand.Import();
    cmd.storageProvider = getMockStorageProvider(preimageStorage);
    cmd.hex = true;
    cmd.preimageFilePath = tempFile;
    cmd.run();

    var preimage = preimageStorage.getRawPreimage(mockAddress.addressHash());
    assertThat(preimage).isPresent();
    assertThat(preimage.get()).isEqualTo(mockAddress);
  }

  private StorageProvider getMockStorageProvider(final WorldStatePreimageStorage wrapped) {
    var mockProvider = mock(StorageProvider.class);
    var kvStorage = ((WorldStatePreimageKeyValueStorage) wrapped).getKeyValueStorage();
    doAnswer(__ -> kvStorage).when(mockProvider).getStorageBySegmentIdentifier(any());
    doAnswer(__ -> wrapped).when(mockProvider).createWorldStatePreimageStorage();
    return mockProvider;
  }
}
