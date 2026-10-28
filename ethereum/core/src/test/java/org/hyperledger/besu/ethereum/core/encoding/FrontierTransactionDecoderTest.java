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
package org.hyperledger.besu.ethereum.core.encoding;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.crypto.SECPSignature;
import org.hyperledger.besu.crypto.SignatureAlgorithm;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.rlp.RLP;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.api.Test;

class FrontierTransactionDecoderTest {

  private static final SignatureAlgorithm SIGNATURE_ALGORITHM =
      SignatureAlgorithmFactory.getInstance();

  private static Bytes legacyTransactionRlpWithV(final BigInteger v) {
    return RLP.encode(
        out -> {
          out.startList();
          out.writeLongScalar(0L); // nonce
          out.writeBigIntegerScalar(BigInteger.ZERO); // gasPrice
          out.writeLongScalar(21_000L); // gasLimit
          out.writeBytes(Bytes.fromHexString("0x1111111111111111111111111111111111111111")); // to
          out.writeBigIntegerScalar(BigInteger.ZERO); // value
          out.writeBytes(Bytes.EMPTY); // payload
          out.writeBigIntegerScalar(v); // v
          out.writeBigIntegerScalar(BigInteger.ONE); // r
          out.writeBigIntegerScalar(BigInteger.ONE); // s
          out.endList();
        });
  }

  @Test
  void rejectsLegacyTransactionWithOverWideVScalar() {
    // A 34-byte v is one byte past the EIP-155 maximum for a 256-bit chainId. It carries its full
    // width into chainId = (v-35)/2, which later renders via superlinear BigInteger.toString() in
    // error/log messages. The decoder must reject it at the RLP layer (like r/s), before any
    // chainId is derived.
    final BigInteger overWideV = BigInteger.ONE.shiftLeft(264); // 2^264 -> 34 bytes, > 33

    assertThatThrownBy(
            () -> FrontierTransactionDecoder.decode(legacyTransactionRlpWithV(overWideV)))
        .isInstanceOf(RLPException.class);
  }

  @Test
  void stillDecodesLegacyTransactionWithNormalV() {
    // EIP-155 v for chainId 1, recId 0: v = 2*1 + 35 = 37. Fits well within 32 bytes.
    final Transaction tx =
        FrontierTransactionDecoder.decode(legacyTransactionRlpWithV(BigInteger.valueOf(37)));

    assertThat(tx.getChainId()).contains(BigInteger.ONE);
  }

  @Test
  void roundTripsLegacyTransactionWith33ByteVForLargeChainId() {
    // EIP-155 v = 2*chainId + 35 + recId, so a near-256-bit chainId yields a v that needs 33 bytes,
    // one more than r/s. Besu accepts such chain ids, the encoder emits the 33-byte v, and the
    // decoder must accept it back — this is the boundary the 32-byte bound wrongly rejected.
    final BigInteger chainId = BigInteger.ONE.shiftLeft(255); // 2^255; v = 2^256 + 35 -> 33 bytes
    final SECPSignature signature =
        SIGNATURE_ALGORITHM.createSignature(BigInteger.ONE, BigInteger.ONE, (byte) 0);
    final Transaction tx =
        Transaction.builder()
            .type(TransactionType.FRONTIER)
            .nonce(0)
            .gasPrice(Wei.ZERO)
            .gasLimit(21_000)
            .to(Address.fromHexString("0x1111111111111111111111111111111111111111"))
            .value(Wei.ZERO)
            .payload(Bytes.EMPTY)
            .chainId(chainId)
            .signature(signature)
            .build();

    // Guard the premise: this chain id really does push v past 32 bytes.
    assertThat(tx.getV().bitLength()).isGreaterThan(256);

    final Bytes encoded = TransactionEncoder.encodeOpaqueBytes(tx, EncodingContext.BLOCK_BODY);
    final Transaction decoded = FrontierTransactionDecoder.decode(encoded);

    assertThat(decoded.getChainId()).contains(chainId);
  }
}
