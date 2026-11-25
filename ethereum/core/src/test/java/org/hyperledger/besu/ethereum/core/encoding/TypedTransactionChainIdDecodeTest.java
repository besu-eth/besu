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

import org.hyperledger.besu.crypto.KeyPair;
import org.hyperledger.besu.crypto.SignatureAlgorithmFactory;
import org.hyperledger.besu.datatypes.Sha256Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.core.TransactionTestFixture;
import org.hyperledger.besu.ethereum.rlp.RLPException;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

/**
 * The chain id of a type 2, 3 or 4 transaction is bounded to 32 bytes at decode. A wider one can
 * never match the network chain id, and rejecting it renders it in decimal, which is superlinear in
 * its width.
 */
class TypedTransactionChainIdDecodeTest {

  private static final KeyPair KEYS = SignatureAlgorithmFactory.getInstance().generateKeyPair();

  private static final BigInteger MAX_32_BYTES =
      BigInteger.ONE.shiftLeft(256).subtract(BigInteger.ONE);
  private static final BigInteger MIN_33_BYTES = BigInteger.ONE.shiftLeft(256);

  private static Bytes encodeWithChainId(final TransactionType type, final BigInteger chainId) {
    final TransactionTestFixture fixture =
        new TransactionTestFixture().type(type).chainId(Optional.of(chainId));
    if (type == TransactionType.BLOB) {
      fixture.versionedHashes(
          Optional.of(
              List.of(
                  new VersionedHash(
                      VersionedHash.SHA256_VERSION_ID, Sha256Hash.sha256(Bytes.of(1))))));
    }
    return TransactionEncoder.encodeOpaqueBytes(
        fixture.createTransaction(KEYS), EncodingContext.BLOCK_BODY);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"EIP1559", "BLOB", "DELEGATE_CODE"})
  void decodesA32ByteChainId(final TransactionType type) {
    final Transaction decoded =
        TransactionDecoder.decodeOpaqueBytes(
            encodeWithChainId(type, MAX_32_BYTES), EncodingContext.BLOCK_BODY);

    assertThat(decoded.getType()).isEqualTo(type);
    assertThat(decoded.getChainId()).contains(MAX_32_BYTES);
  }

  @ParameterizedTest
  @EnumSource(
      value = TransactionType.class,
      names = {"EIP1559", "BLOB", "DELEGATE_CODE"})
  void rejectsA33ByteChainId(final TransactionType type) {
    final Bytes encoded = encodeWithChainId(type, MIN_33_BYTES);

    assertThatThrownBy(
            () -> TransactionDecoder.decodeOpaqueBytes(encoded, EncodingContext.BLOCK_BODY))
        .isInstanceOf(RLPException.class);
  }
}
