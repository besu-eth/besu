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

import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_PROTECTED_V_BASE;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_PROTECTED_V_MIN;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_UNPROTECTED_V_BASE;
import static org.hyperledger.besu.ethereum.core.Transaction.REPLAY_UNPROTECTED_V_BASE_PLUS_1;
import static org.hyperledger.besu.ethereum.core.Transaction.TWO;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPInput;

import java.math.BigInteger;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

public class FrontierTransactionDecoder {
  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  public static Transaction decode(final Bytes input) {
    return decode(RLP.input(input));
  }

  public static Transaction decode(final RLPInput transactionRlp) {
    int size = transactionRlp.currentSize();
    transactionRlp.enterList();
    final Transaction.Builder builder =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(transactionRlp.readLongScalar())
            .gasPrice(Wei.of(transactionRlp.readUInt256Scalar()))
            .gasLimit(transactionRlp.readLongScalar())
            .to(transactionRlp.readBytes(v -> v.isEmpty() ? null : Address.wrap(v)))
            .value(Wei.of(transactionRlp.readUInt256Scalar()))
            .payload(transactionRlp.readBytes())
            .rawRlp(transactionRlp.raw())
            .sizeForBlockInclusion(size)
            .sizeForAnnouncement(size)
            .hash(Hash.hash(transactionRlp.raw()));

    final BigInteger v = transactionRlp.readBigIntegerScalar();
    final byte recId;
    Optional<BigInteger> chainId = Optional.empty();
    if (v.equals(REPLAY_UNPROTECTED_V_BASE) || v.equals(REPLAY_UNPROTECTED_V_BASE_PLUS_1)) {
      recId = v.subtract(REPLAY_UNPROTECTED_V_BASE).byteValueExact();
    } else if (v.compareTo(REPLAY_PROTECTED_V_MIN) > 0) {
      chainId = Optional.of(v.subtract(REPLAY_PROTECTED_V_BASE).divide(TWO));
      recId = v.subtract(TWO.multiply(chainId.get()).add(REPLAY_PROTECTED_V_BASE)).byteValueExact();
    } else {
      throw new RuntimeException(
          String.format("An unsupported encoded `v` value of %s was found", v));
    }
    final BigInteger r = transactionRlp.readUInt256Scalar().toUnsignedBigInteger();
    final BigInteger s = transactionRlp.readUInt256Scalar().toUnsignedBigInteger();
    final SECPSignature signature = SIGNATURE_ALGORITHM.createSignature(r, s, recId);

    transactionRlp.leaveList();

    chainId.ifPresent(builder::chainId);
    return builder.signature(signature).build();
  }
}
