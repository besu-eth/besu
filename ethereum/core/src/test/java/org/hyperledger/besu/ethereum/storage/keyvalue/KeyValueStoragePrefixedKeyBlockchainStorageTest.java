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
package org.hyperledger.besu.ethereum.storage.keyvalue;

import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.CHAIN_HEAD_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.FINALIZED_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys.SAFE_BLOCK_HASH;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.SAMPLE_CHAIN_HEAD;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.assertNoVariablesInStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.assertVariablesPresentInVariablesStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.assertVariablesReturnedByBlockchainStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.getSampleVariableValues;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.populateBlockchainStorage;
import static org.hyperledger.besu.ethereum.core.VariablesStorageHelper.populateVariablesStorage;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.ethereum.chain.BlockchainStorage.Updater;
import org.hyperledger.besu.ethereum.chain.VariablesStorage;
import org.hyperledger.besu.ethereum.chain.VariablesStorage.Keys;
import org.hyperledger.besu.ethereum.core.BlockDataGenerator;
import org.hyperledger.besu.ethereum.core.BlockHeaderFunctions;
import org.hyperledger.besu.ethereum.core.SyncTransactionReceipt;
import org.hyperledger.besu.ethereum.core.TransactionReceipt;
import org.hyperledger.besu.ethereum.core.encoding.receipt.SyncTransactionReceiptDecoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncoder;
import org.hyperledger.besu.ethereum.core.encoding.receipt.TransactionReceiptEncodingConfiguration;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPInput;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.plugin.services.storage.KeyValueStorage;
import org.hyperledger.besu.services.kvstore.InMemoryKeyValueStorage;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class KeyValueStoragePrefixedKeyBlockchainStorageTest {
  private final BlockHeaderFunctions blockHeaderFunctions = mock(BlockHeaderFunctions.class);
  private KeyValueStorage kvBlockchain;
  private KeyValueStorage kvVariables;
  private VariablesStorage variablesStorage;
  private Map<Keys, Bytes> variableValues;

  @BeforeEach
  public void setup() {
    kvBlockchain = new InMemoryKeyValueStorage();
    kvVariables = new InMemoryKeyValueStorage();
    variablesStorage = new VariablesKeyValueStorage(kvVariables);
    variableValues = getSampleVariableValues();
  }

  @Test
  public void migrationToVariablesStorage() {
    populateBlockchainStorage(kvBlockchain, variableValues);

    assertNoVariablesInStorage(kvVariables);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    assertNoVariablesInStorage(kvBlockchain);
    assertVariablesPresentInVariablesStorage(kvVariables, variableValues);

    assertVariablesReturnedByBlockchainStorage(blockchainStorage, variableValues);
  }

  @Test
  public void migrationToVariablesStorageWhenSomeVariablesDoNotExist() {
    variableValues.remove(FINALIZED_BLOCK_HASH);
    variableValues.remove(SAFE_BLOCK_HASH);
    populateBlockchainStorage(kvBlockchain, variableValues);

    assertNoVariablesInStorage(kvVariables);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    assertNoVariablesInStorage(kvBlockchain);
    assertVariablesPresentInVariablesStorage(kvVariables, variableValues);

    assertVariablesReturnedByBlockchainStorage(blockchainStorage, variableValues);
  }

  @Test
  public void doesNothingIfVariablesAlreadyMigrated() {
    populateVariablesStorage(kvVariables, variableValues);

    assertNoVariablesInStorage(kvBlockchain);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    assertNoVariablesInStorage(kvBlockchain);
    assertVariablesPresentInVariablesStorage(kvVariables, variableValues);

    assertVariablesReturnedByBlockchainStorage(blockchainStorage, variableValues);
  }

  @Test
  public void failIfInconsistencyDetectedDuringVariablesMigration() {
    populateBlockchainStorage(kvBlockchain, variableValues);
    // create and inconsistency putting a different chain head in variables storage
    variableValues.put(CHAIN_HEAD_HASH, SAMPLE_CHAIN_HEAD.getBytes().shiftLeft(1));
    populateVariablesStorage(kvVariables, variableValues);
    Assertions.assertThrows(
        IllegalStateException.class,
        () ->
            new KeyValueStoragePrefixedKeyBlockchainStorage(
                kvBlockchain, variablesStorage, blockHeaderFunctions, false));
  }

  @Test
  public void testUpdaterPutTransactionReceipts() {
    populateBlockchainStorage(kvBlockchain, variableValues);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    BlockDataGenerator generator = new BlockDataGenerator();
    Hash blockHash = generator.hash();
    List<TransactionReceipt> transactionReceipts = new ArrayList<>();
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());

    Updater updater = blockchainStorage.updater();
    updater.putTransactionReceipts(blockHash, transactionReceipts);
    updater.commit();

    List<TransactionReceipt> loadedReceipts =
        blockchainStorage.getTransactionReceipts(blockHash).get();
    Assertions.assertEquals(4, loadedReceipts.size());
    Assertions.assertTrue(loadedReceipts.containsAll(transactionReceipts));
  }

  @Test
  public void testUpdaterPutSyncTransactionReceipts() {
    populateBlockchainStorage(kvBlockchain, variableValues);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    BlockDataGenerator generator = new BlockDataGenerator();
    SyncTransactionReceiptDecoder syncReceiptDecoder = new SyncTransactionReceiptDecoder();

    Hash blockHash = generator.hash();
    List<TransactionReceipt> transactionReceipts = new ArrayList<>();
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());
    transactionReceipts.add(generator.receipt());
    List<SyncTransactionReceipt> syncReceipts =
        transactionReceipts.stream()
            .map(
                (tr) -> {
                  BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
                  TransactionReceiptEncoder.writeTo(
                      tr, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT);
                  return syncReceiptDecoder.decode(rlpOut.encoded());
                })
            .toList();

    Updater updater = blockchainStorage.updater();
    updater.putSyncTransactionReceipts(blockHash, syncReceipts);
    updater.commit();

    List<TransactionReceipt> loadedReceipts =
        blockchainStorage.getTransactionReceipts(blockHash).get();
    Assertions.assertEquals(4, loadedReceipts.size());
    Assertions.assertTrue(loadedReceipts.containsAll(transactionReceipts));
  }

  /**
   * Tests that typed (EIP-1559) receipts received from the network via snap sync are stored and
   * retrieved correctly. On the network, typed receipts are transmitted as RLP bytes elements;
   * ReceiptsMessage.deserializeReceiptLists() calls readBytes() which strips the RLP wrapper,
   * leaving raw typeCode||rlp_body bytes. This simulates that path and verifies round-trip
   * correctness through putSyncTransactionReceipts / getTransactionReceipts.
   */
  @Test
  public void testUpdaterPutSyncTransactionReceipts_typedReceiptsFromNetwork() {
    populateBlockchainStorage(kvBlockchain, variableValues);

    final var blockchainStorage =
        new KeyValueStoragePrefixedKeyBlockchainStorage(
            kvBlockchain, variablesStorage, blockHeaderFunctions, false);

    BlockDataGenerator generator = new BlockDataGenerator();
    SyncTransactionReceiptDecoder syncReceiptDecoder = new SyncTransactionReceiptDecoder();

    Hash blockHash = generator.hash();
    // Create explicitly typed (EIP-1559) receipts to exercise the typed-receipt storage path
    List<TransactionReceipt> transactionReceipts =
        List.of(
            new TransactionReceipt(
                TransactionType.EIP1559, 1, 1000L, List.of(), java.util.Optional.empty()),
            new TransactionReceipt(
                TransactionType.ACCESS_LIST, 1, 2000L, List.of(), java.util.Optional.empty()));

    List<SyncTransactionReceipt> syncReceipts =
        transactionReceipts.stream()
            .map(
                tr -> {
                  // Encode as it would appear on the wire (eth/68): an RLP bytes element
                  BytesValueRLPOutput rlpOut = new BytesValueRLPOutput();
                  TransactionReceiptEncoder.writeTo(
                      tr, rlpOut, TransactionReceiptEncodingConfiguration.DEFAULT);
                  // Simulate ReceiptsMessage.deserializeReceiptLists() calling readBytes(),
                  // which strips the outer RLP bytes-element wrapper and returns the raw
                  // typeCode||rlp_body content (first byte in 0x01-0x7f)
                  final Bytes rawWireContent =
                      new BytesValueRLPInput(rlpOut.encoded(), false).readBytes();
                  return syncReceiptDecoder.decode(rawWireContent);
                })
            .toList();

    Updater updater = blockchainStorage.updater();
    updater.putSyncTransactionReceipts(blockHash, syncReceipts);
    updater.commit();

    List<TransactionReceipt> loadedReceipts =
        blockchainStorage.getTransactionReceipts(blockHash).get();
    Assertions.assertEquals(2, loadedReceipts.size());
    Assertions.assertTrue(loadedReceipts.containsAll(transactionReceipts));
  }
}
