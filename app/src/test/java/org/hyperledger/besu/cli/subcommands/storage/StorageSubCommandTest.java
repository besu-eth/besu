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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FINALIZED_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SAFE_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.assertNoVariablesInStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.assertVariablesPresentInBlockchainStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.getSampleVariableValues;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.populateBlockchainStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.populateVariablesStorage;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.BLOCKCHAIN;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.CODE_STORAGE;
import static org.hyperledger.besu.ethereum.storage.keyvalue.KeyValueSegmentIdentifier.VARIABLES;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.cli.CommandTestAbstract;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.storage.keyvalue.VariablesKeyValueStorage;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.storage.code.CodeStorageMigration;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.BaseVersionedStorageFormat;
import org.hyperledger.besu.plugin.services.storage.rocksdb.configuration.DatabaseMetadata;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedInMemoryKeyValueStorage;
import org.hyperledger.besu.services.kvstore.SegmentedKeyValueStorageAdapter;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class StorageSubCommandTest extends CommandTestAbstract {

  @Test
  public void storageSubCommandExists() {
    parseCommand("storage");

    assertThat(commandOutput.toString(UTF_8))
        .contains("This command provides storage related actions");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void storageRevertVariablesSubCommandExists() {
    parseCommand("storage", "revert-variables", "--help");

    assertThat(commandOutput.toString(UTF_8))
        .contains("This command revert the modifications done by the variables storage feature");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void storageRevertCodeFormatSubCommandExists() {
    parseCommand("storage", "revert-code-format", "--help");

    assertThat(commandOutput.toString(UTF_8))
        .contains("Revert the contract code storage to the format read by Besu versions before");
    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
  }

  @Test
  public void revertCodeFormat(@TempDir final Path dataDir) throws IOException {
    final var codeStorage = new SegmentedInMemoryKeyValueStorage();
    when(storageProvider.getStorageBySegmentIdentifiers(List.of(CODE_STORAGE)))
        .thenReturn(codeStorage);
    final Bytes code = Bytes.fromHexString("0x605b5b");
    final byte[] key = Hash.hash(code).getBytes().toArrayUnsafe();
    final var setup = codeStorage.startTransaction();
    setup.put(CODE_STORAGE, key, code.toArrayUnsafe());
    setup.commit();
    CodeStorageMigration.migrate(codeStorage);
    new DatabaseMetadata(BaseVersionedStorageFormat.BONSAI_WITH_CODE_FORMAT)
        .writeToDirectory(dataDir);

    parseCommand("--data-path", dataDir.toString(), "storage", "revert-code-format");

    assertThat(commandErrorOutput.toString(UTF_8)).isEmpty();
    assertThat(codeStorage.get(CODE_STORAGE, key)).contains(code.toArrayUnsafe());
    assertThat(codeStorage.get(CODE_STORAGE, CodeStorageMigration.FORMAT_KEY)).isEmpty();
    assertThat(DatabaseMetadata.lookUpFrom(dataDir).getVersionedStorageFormat())
        .isEqualTo(BaseVersionedStorageFormat.BONSAI_WITH_RECEIPT_COMPACTION);
  }

  @Test
  public void revertVariables() {
    final var kvVariablesSeg = new SegmentedInMemoryKeyValueStorage();
    final var kvVariables = new SegmentedKeyValueStorageAdapter(VARIABLES, kvVariablesSeg);
    final var kvBlockchainSeg = new SegmentedInMemoryKeyValueStorage();
    final var kvBlockchain = new SegmentedKeyValueStorageAdapter(BLOCKCHAIN, kvBlockchainSeg);
    when(storageProvider.createVariablesStorage())
        .thenReturn(new VariablesKeyValueStorage(kvVariables));
    when(storageProvider.getStorageBySegmentIdentifier(BLOCKCHAIN)).thenReturn(kvBlockchain);

    final var variableValues = getSampleVariableValues();
    assertNoVariablesInStorage(kvBlockchain);
    populateVariablesStorage(kvVariables, variableValues);

    parseCommand("storage", "revert-variables");

    assertNoVariablesInStorage(kvVariables);
    assertVariablesPresentInBlockchainStorage(kvBlockchain, variableValues);
  }

  @Test
  public void revertVariablesWhenSomeVariablesDoNotExist() {
    final var kvVariablesSeg = new SegmentedInMemoryKeyValueStorage();
    final var kvVariables = new SegmentedKeyValueStorageAdapter(VARIABLES, kvVariablesSeg);
    final var kvBlockchainSeg = new SegmentedInMemoryKeyValueStorage();
    final var kvBlockchain = new SegmentedKeyValueStorageAdapter(BLOCKCHAIN, kvBlockchainSeg);
    when(storageProvider.createVariablesStorage())
        .thenReturn(new VariablesKeyValueStorage(kvVariables));
    when(storageProvider.getStorageBySegmentIdentifier(BLOCKCHAIN)).thenReturn(kvBlockchain);

    final var variableValues = getSampleVariableValues();
    variableValues.remove(FINALIZED_BLOCK_HASH);
    variableValues.remove(SAFE_BLOCK_HASH);
    assertNoVariablesInStorage(kvBlockchain);
    populateVariablesStorage(kvVariables, variableValues);

    parseCommand("storage", "revert-variables");

    assertNoVariablesInStorage(kvVariables);
    assertVariablesPresentInBlockchainStorage(kvBlockchain, variableValues);
  }

  @Test
  public void doesNothingWhenVariablesAlreadyReverted() {
    final var kvVariables = new InMemoryKeyValueStorage();
    final var kvBlockchain = new InMemoryKeyValueStorage();
    when(storageProvider.createVariablesStorage())
        .thenReturn(new VariablesKeyValueStorage(kvVariables));
    when(storageProvider.getStorageBySegmentIdentifier(BLOCKCHAIN)).thenReturn(kvBlockchain);

    final var variableValues = getSampleVariableValues();
    assertNoVariablesInStorage(kvVariables);
    populateBlockchainStorage(kvBlockchain, variableValues);

    parseCommand("storage", "revert-variables");

    assertNoVariablesInStorage(kvVariables);
    assertVariablesPresentInBlockchainStorage(kvBlockchain, variableValues);
  }
}
