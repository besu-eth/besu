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
package org.hyperledger.besu.ethereum.blockcreation.txselection.selectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction.MAX_SCORE;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.BLOB_PRICE_BELOW_CURRENT_MIN;
import static org.hyperledger.besu.plugin.data.TransactionSelectionResult.SELECTED;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.blockcreation.txselection.BlockSelectionContext;
import org.hyperledger.besu.ethereum.blockcreation.txselection.TransactionEvaluationContext;
import org.hyperledger.besu.ethereum.core.Frame;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.eth.transactions.PendingTransaction;
import org.hyperledger.besu.plugin.data.TransactionSelectionResult;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class BlobPriceTransactionSelectorTest {

  private static final KeyPair KEYS = SignatureAlgorithmFactory.getInstance().generateKeyPair();
  private static final Wei BLOB_GAS_PRICE = Wei.of(10);
  private static final List<VersionedHash> ONE_BLOB =
      List.of(new VersionedHash((byte) 1, Hash.ZERO));

  private final BlobPriceTransactionSelector selector =
      new BlobPriceTransactionSelector(
          new BlockSelectionContext(null, null, null, BLOB_GAS_PRICE, null, null));

  @Test
  void blobTransactionBelowTheBlobPriceIsNotSelected() {
    assertThat(evaluate(blobTransaction(Wei.of(9)))).isEqualTo(BLOB_PRICE_BELOW_CURRENT_MIN);
  }

  @Test
  void blobTransactionAtOrAboveTheBlobPriceIsSelected() {
    assertThat(evaluate(blobTransaction(BLOB_GAS_PRICE))).isEqualTo(SELECTED);
    assertThat(evaluate(blobTransaction(Wei.of(11)))).isEqualTo(SELECTED);
  }

  @Test
  void nonBlobTransactionIsSelected() {
    final Transaction eip1559 =
        new TransactionTestFixture()
            .type(TransactionType.EIP1559)
            .to(Optional.of(Address.ZERO))
            .createTransaction(KEYS);
    assertThat(evaluate(eip1559)).isEqualTo(SELECTED);
  }

  @Test
  void frameTransactionWithoutBlobsIgnoresItsBlobFeeCap() {
    assertThat(evaluate(frameTransaction(Wei.ZERO, null))).isEqualTo(SELECTED);
  }

  @Test
  void frameTransactionCarryingBlobsIsPricedLikeABlobTransaction() {
    assertThat(evaluate(frameTransaction(Wei.of(9), ONE_BLOB)))
        .isEqualTo(BLOB_PRICE_BELOW_CURRENT_MIN);
    assertThat(evaluate(frameTransaction(BLOB_GAS_PRICE, ONE_BLOB))).isEqualTo(SELECTED);
  }

  @Test
  void postProcessingAlwaysSelects() {
    final TransactionEvaluationContext context = context(blobTransaction(Wei.of(9)));
    assertThat(selector.evaluateTransactionPostProcessing(context, null)).isEqualTo(SELECTED);
  }

  private TransactionSelectionResult evaluate(final Transaction transaction) {
    return selector.evaluateTransactionPreProcessing(context(transaction));
  }

  private static TransactionEvaluationContext context(final Transaction transaction) {
    return new TransactionEvaluationContext(
        null,
        PendingTransaction.newPendingTransaction(transaction, false, false, MAX_SCORE),
        null,
        null,
        null,
        () -> false);
  }

  private static Transaction blobTransaction(final Wei maxFeePerBlobGas) {
    return new TransactionTestFixture()
        .type(TransactionType.BLOB)
        .to(Optional.of(Address.ZERO))
        .maxFeePerBlobGas(Optional.of(maxFeePerBlobGas))
        .versionedHashes(Optional.of(ONE_BLOB))
        .createTransaction(KEYS);
  }

  private static Transaction frameTransaction(
      final Wei maxFeePerBlobGas, final List<VersionedHash> versionedHashes) {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(BigInteger.ONE)
        .nonce(0)
        .sender(Address.ZERO)
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_SENDER, 0, Optional.empty(), 21_000L, 0L, Wei.ZERO, Bytes.EMPTY)))
        .maxPriorityFeePerGas(Wei.of(1))
        .maxFeePerGas(Wei.of(10))
        .maxFeePerBlobGas(maxFeePerBlobGas)
        .versionedHashes(versionedHashes)
        .build();
  }
}
