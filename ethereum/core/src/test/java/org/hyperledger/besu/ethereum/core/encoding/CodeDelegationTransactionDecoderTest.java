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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.CodeDelegation;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.BytesValueRLPOutput;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class CodeDelegationTransactionDecoderTest {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();
  private static final KeyPair KEYS = SIGNATURE_ALGORITHM.generateKeyPair();

  @Test
  void decodesAuthorizationListWithinLimit() {
    final Bytes encoded = setCodeTransaction(75_000, 3);

    final Transaction transaction = CodeDelegationTransactionDecoder.decode(encoded, gasLimit -> 3);

    assertThat(transaction.codeDelegationListSize()).isEqualTo(3);
  }

  @Test
  void refusesAuthorizationListOverLimit() {
    final Bytes encoded = setCodeTransaction(50_000, 3);

    assertThatThrownBy(() -> CodeDelegationTransactionDecoder.decode(encoded, gasLimit -> 2))
        .isInstanceOf(RLPException.class)
        .hasMessageContaining("maximum permitted size of 2");
  }

  @Test
  void limitIsAskedForTheTransactionGasLimit() {
    final AtomicLong seenGasLimit = new AtomicLong();

    CodeDelegationTransactionDecoder.decode(
        setCodeTransaction(123_456, 1),
        gasLimit -> {
          seenGasLimit.set(gasLimit);
          return 1;
        });

    assertThat(seenGasLimit).hasValue(123_456);
  }

  @Test
  void decodesWithoutLimitByDefault() {
    final Transaction transaction =
        CodeDelegationTransactionDecoder.decode(setCodeTransaction(0, 3));

    assertThat(transaction.codeDelegationListSize()).isEqualTo(3);
  }

  @Test
  void transactionDecoderPassesLimitToCodeDelegationDecoder() {
    final Transaction transaction =
        TransactionDecoder.decodeOpaqueBytes(
            setCodeTransaction(0, 3), EncodingContext.POOLED_TRANSACTION);
    final BytesValueRLPOutput out = new BytesValueRLPOutput();
    TransactionEncoder.encodeRLP(transaction, out, EncodingContext.POOLED_TRANSACTION);

    assertThatThrownBy(
            () ->
                TransactionDecoder.decodeRLP(
                    RLP.input(out.encoded()), EncodingContext.POOLED_TRANSACTION, gasLimit -> 0))
        .isInstanceOf(RLPException.class);
    assertThat(
            TransactionDecoder.decodeRLP(
                    RLP.input(out.encoded()), EncodingContext.POOLED_TRANSACTION, gasLimit -> 3)
                .codeDelegationListSize())
        .isEqualTo(3);
  }

  static Bytes setCodeTransaction(final long gasLimit, final int authorizations) {
    final CodeDelegation delegation =
        new org.hyperledger.besu.ethereum.core.CodeDelegation(
            BigInteger.ONE,
            Address.fromHexString("0x0000000000000000000000000000000000001000"),
            0,
            SIGNATURE_ALGORITHM.createCodeDelegationSignature(
                BigInteger.ONE, BigInteger.ONE, (byte) 0));
    final List<CodeDelegation> delegations = Collections.nCopies(authorizations, delegation);
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.DELEGATE_CODE)
            .chainId(BigInteger.ONE)
            .nonce(0)
            .maxPriorityFeePerGas(Wei.of(1))
            .maxFeePerGas(Wei.of(1))
            .gasLimit(gasLimit)
            .to(Address.ZERO)
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .accessList(List.of())
            .codeDelegations(delegations)
            .signAndBuild(KEYS);
    return TransactionEncoder.encodeOpaqueBytes(transaction, EncodingContext.BLOCK_BODY);
  }
}
