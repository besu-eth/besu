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
package org.hyperledger.besu.ethereum.referencetests;

import org.hyperledger.besu.config.BlobScheduleOptions;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;

import java.util.List;
import java.util.Optional;
import java.util.function.Supplier;

public class GeneralStateTestCaseEipSpec {

  private final String fork;

  // Creating the actual transaction is expensive because the json test file does not give us the
  // transaction but rather the private key to sign, and so we have to do the signing. And we don't
  // want to do this for 22k general state tests up-front (during
  // GeneralStateReferenceTest.getTestParametersForConfig) because 1) that makes the parameters
  // generation of
  // GeneralStateReferenceTest take more than a minute, which means that much time waiting before
  // anything
  // is run, which isn't friendly and 2) this makes it harder to parallelize this step. Anyway, this
  // is why this is a supplier: calling get() actually does the signing.
  private final List<Supplier<Transaction>> transactionSuppliers;

  private final ReferenceTestWorldState initialWorldState;

  private final Hash expectedRootHash;

  // The keccak256 hash of the RLP encoding of the log series
  private final Hash expectedLogsHash;

  private final BlockHeader blockHeader;

  private final int dataIndex;
  private final int gasIndex;
  private final int valueIndex;
  private final String expectException;
  private final Optional<BlobScheduleOptions> blobScheduleOptions;

  public GeneralStateTestCaseEipSpec(
      final String fork,
      final List<Supplier<Transaction>> transactionSuppliers,
      final ReferenceTestWorldState initialWorldState,
      final Hash expectedRootHash,
      final Hash expectedLogsHash,
      final BlockHeader blockHeader,
      final int dataIndex,
      final int gasIndex,
      final int valueIndex,
      final String expectException,
      final Optional<BlobScheduleOptions> blobScheduleOptions) {
    this.fork = fork;
    this.transactionSuppliers = transactionSuppliers;
    this.initialWorldState = initialWorldState;
    this.expectedRootHash = expectedRootHash;
    this.expectedLogsHash = expectedLogsHash;
    this.blockHeader = blockHeader;
    this.dataIndex = dataIndex;
    this.gasIndex = gasIndex;
    this.valueIndex = valueIndex;
    this.expectException = expectException;
    this.blobScheduleOptions = blobScheduleOptions;
  }

  public String getFork() {
    return fork;
  }

  public ReferenceTestWorldState getInitialWorldState() {
    return initialWorldState;
  }

  public Hash getExpectedRootHash() {
    return expectedRootHash;
  }

  public Hash getExpectedLogsHash() {
    return expectedLogsHash;
  }

  public int getTransactionsCount() {
    return transactionSuppliers.size();
  }

  public Transaction getTransaction(final int txIndex) {
    try {
      return transactionSuppliers.get(txIndex).get();
    } catch (RuntimeException re) {
      // some tests specify invalid transactions.  We throw exceptions in
      // GeneralStateTests but they are encoded in BlockchainTests, so we
      // can skip them as invalid (since the point of the tests is to reject
      // invalid transactions).
      return null;
    }
  }

  public BlockHeader getBlockHeader() {
    return blockHeader;
  }

  public int getDataIndex() {
    return dataIndex;
  }

  public int getGasIndex() {
    return gasIndex;
  }

  public int getValueIndex() {
    return valueIndex;
  }

  public String getExpectException() {
    return expectException;
  }

  /**
   * The blob schedule from the fixture's {@code config.blobSchedule}, which sets the blob target,
   * max and base fee update fraction the test's fork runs with.
   *
   * @return the blob schedule options, or empty when the fixture carries no blob schedule
   */
  public Optional<BlobScheduleOptions> getBlobScheduleOptions() {
    return blobScheduleOptions;
  }
}
