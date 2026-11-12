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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOB_PRICE_BELOW_CURRENT_MIN;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class BlobPriceTransactionSelectorTest {
  private static final KeyPair KEYS = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private static final Wei BLOB_GAS_PRICE = Wei.of(10);
  private static final Optional<List<VersionedHash>> ONE_BLOB =
      Optional.of(List.of(new VersionedHash((byte) 1, Hash.ZERO)));

  private final BlobPriceTransactionSelector selector =
      new BlobPriceTransactionSelector(
          new BlockSelectionContext(null, null, null, BLOB_GAS_PRICE, null, null));

  static Stream<Arguments> transactions() {
    return Stream.of(
        Arguments.of("blob tx at the blob gas price", blobTransaction(BLOB_GAS_PRICE), SELECTED),
        Arguments.of(
            "blob tx below the blob gas price",
            blobTransaction(BLOB_GAS_PRICE.subtract(1)),
            BLOB_PRICE_BELOW_CURRENT_MIN),
        Arguments.of("frame tx without blobs", frameTransaction(Optional.empty()), SELECTED),
        Arguments.of("frame tx with a blob", frameTransaction(ONE_BLOB), SELECTED),
        Arguments.of(
            "non-blob tx",
            new TransactionTestFixture().type(TransactionType.EIP1559).createTransaction(KEYS),
            SELECTED));
  }

  @ParameterizedTest(name = "{0}")
  @MethodSource("transactions")
  void selectsOnlyTransactionsWhoseMaxFeePerBlobGasCoversTheBlobGasPrice(
      final String description,
      final Transaction transaction,
      final TransactionSelectionResult expected) {
    final var evaluationContext =
        new TransactionEvaluationContext(
            null,
            PendingTransaction.newPendingTransaction(transaction, false, false, MAX_SCORE),
            null,
            null,
            null,
            () -> false);

    assertThat(selector.evaluateTransactionPreProcessing(evaluationContext)).isEqualTo(expected);
  }

  private static Transaction blobTransaction(final Wei maxFeePerBlobGas) {
    return new TransactionTestFixture()
        .type(TransactionType.BLOB)
        .maxFeePerBlobGas(Optional.of(maxFeePerBlobGas))
        .versionedHashes(ONE_BLOB)
        .createTransaction(KEYS);
  }

  private static Transaction frameTransaction(final Optional<List<VersionedHash>> versionedHashes) {
    return new TransactionTestFixture()
        .type(TransactionType.FRAME)
        .versionedHashes(versionedHashes)
        .createTransaction(KEYS);
  }
}
