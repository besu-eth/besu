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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionEncoder;
import org.hyperledger.besu.evm.frame.KeyedNonces;

import java.math.BigInteger;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.MethodSource;

class FrameTransactionGasTest {

  private static final BigInteger CHAIN_ID = BigInteger.valueOf(7_038_851_975L);

  /**
   * A self-verified transfer with the nonce keys [0xbeef], shaped like a bogota-devnet-1 spamoor
   * transaction: a VERIFY frame approving both scopes and a 1 wei SENDER frame, with one SECP256K1
   * entry.
   */
  private static Transaction keyedTransaction() {
    final Address sender = Address.fromHexString("0xbe59fef26b0b6cee61e6125b265130b8fa50d597");
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonceKeys(List.of(UInt256.valueOf(0xbeef)))
        .nonce(0)
        .sender(sender)
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.empty(),
                    50_000L,
                    97_920L,
                    Wei.ZERO,
                    Bytes.EMPTY),
                new Frame(
                    Frame.MODE_SENDER,
                    0,
                    Optional.of(Address.fromHexString("0xdead")),
                    50_000L,
                    183_600L,
                    Wei.ONE,
                    Bytes.EMPTY)))
        .frameSignatures(
            List.of(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    sender.getBytes(),
                    Bytes.EMPTY,
                    Bytes.fromHexString(
                        "0x01e9347ef4d93ed05357f580423f6b4f467e076b14a53ed2660887cfaf37893fda15a862"
                            + "61e0927c6a2eb4c7cbb475a14c3ca7cfab4f5e2a33aeee7b85825650d8"))))
        .maxFee(Wei.fromEth(1))
        .maxPriorityFeePerGas(Wei.of(1_000_000_000L))
        .build();
  }

  /**
   * A transfer behind an expiry verifier frame with the empty (legacy) key set, shaped like a
   * bogota-devnet-1 spamoor transaction: the expiry frame, a VERIFY frame approving both scopes and
   * a SENDER frame, with one SECP256K1 entry.
   */
  private static Transaction legacyTransaction() {
    return Transaction.builder()
        .type(TransactionType.FRAME)
        .chainId(CHAIN_ID)
        .nonce(666)
        .sender(Address.fromHexString("0x16f7d57da54f962220a557858b134a14fdae43ab"))
        .frames(
            List.of(
                new Frame(
                    Frame.MODE_VERIFY,
                    0,
                    Optional.of(
                        Address.fromHexString("0x81413f0cf12e9b6a49b1d0439e081c577d57ffff")),
                    5_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.fromHexString("0x000000006ad1479e")),
                new Frame(
                    Frame.MODE_VERIFY,
                    Frame.APPROVE_SCOPE_MASK,
                    Optional.empty(),
                    5_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY),
                new Frame(
                    Frame.MODE_SENDER,
                    0,
                    Optional.of(
                        Address.fromHexString("0xf49995b30fab79d05f1c1b54e9f9d2e1fcc812ad")),
                    30_000L,
                    0L,
                    Wei.ZERO,
                    Bytes.EMPTY)))
        .frameSignatures(
            List.of(
                new FrameSignature(
                    FrameSignature.SCHEME_SECP256K1,
                    Bytes.EMPTY,
                    Bytes.EMPTY,
                    Bytes.fromHexString(
                        "0x01177d11083ec49b2d969a9f20b7cca634c1a204c2fba9e5d82ca262976b56093d3ce316"
                            + "721346ce060595b081d944226f997ec30b81dad6faa405d1609b3e0a62"))))
        .maxFee(Wei.fromEth(1))
        .maxPriorityFeePerGas(Wei.of(2_000_000_000L))
        .build();
  }

  @Test
  void keyedNonceAccessCostIsColdStorageAccessPlusStorageWrite() {
    assertThat(FrameTransactionGas.KEYED_NONCE_ACCESS_COST).isEqualTo(12_100L);
  }

  @Test
  void legacyKeySetPaysNoKeyedNonceAccess() {
    assertThat(FrameTransactionGas.keyedNonceAccessGas(KeyedNonces.LEGACY_NONCE_KEYS)).isZero();
  }

  @Test
  void eachKeyOfANonLegacySetPaysTheAccessCost() {
    assertThat(FrameTransactionGas.keyedNonceAccessGas(List.of(UInt256.valueOf(0xbeef))))
        .isEqualTo(12_100L);
    assertThat(
            FrameTransactionGas.keyedNonceAccessGas(
                List.of(UInt256.valueOf(1), UInt256.valueOf(2), UInt256.valueOf(3))))
        .isEqualTo(36_300L);
    // EIP-8250: the zero key is an ordinary keyed nonce and pays like any other.
    assertThat(FrameTransactionGas.keyedNonceAccessGas(List.of(UInt256.ZERO))).isEqualTo(12_100L);
    assertThat(FrameTransactionGas.keyedNonceAccessGas(List.of(UInt256.ZERO, UInt256.valueOf(1))))
        .isEqualTo(24_200L);
  }

  /** Without the keyed nonce access cost, the intrinsic gas and max_gas come out 12,100 short. */
  @Test
  void keyedTransactionPaysTheKeyedNonceAccessCost() {
    final Transaction transaction = keyedTransaction();

    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(35_290L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(39_610L);
    assertThat(transaction.getGasLimit()).isEqualTo(416_810L);
  }

  @Test
  void legacyTransactionPaysNoKeyedNonceAccessCost() {
    final Transaction transaction = legacyTransaction();

    // The nonce calldata is rlp([]) ++ rlp(666): 0xc0 0x82029a, four non-zero bytes.
    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(17_409L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(21_153L);
    assertThat(transaction.getGasLimit()).isEqualTo(57_409L);
  }

  @Test
  void sixteenKeyTransactionPaysTheAccessCostOfEveryKey() {
    final Transaction transaction = KeyedNonceGasCapVectors.atCapStandard();

    // Base 12,000 + one frame 475 + 16 keys x 12,100 = 206,075, plus the 18 non-zero bytes of the
    // nonce calldata: 18 x 16 = 288 as intrinsic calldata, 18 x 64 = 1,152 under the floor.
    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(206_363L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(207_227L);
  }

  /**
   * The cost of a 32-byte ARBITRARY signature over an empty one, after test_elided_signatures.py::
   * test_elided_signature_priced_by_length: an empty-msg entry pays 16 gas per byte, zero or not,
   * while an explicit-msg entry keeps the 4 gas price of a zero byte. The floor prices every byte
   * at 64 gas either way.
   */
  @ParameterizedTest(name = "{0}")
  @CsvSource({
    "empty_msg-zero_bytes, 0x, 0x00, 512",
    "empty_msg-nonzero_bytes, 0x, 0xab, 512",
    "explicit_msg-zero_bytes, 0x5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a, 0x00, 128",
    "explicit_msg-nonzero_bytes, 0x5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a5a, 0xab, 512"
  })
  void elidedSignatureBytesArePricedByLength(
      final String name, final String msg, final String fill, final long expectedIntrinsicCost) {
    final Address sender = Address.fromHexString("0x1111111111111111111111111111111111111111");
    final List<Frame> frames =
        List.of(
            new Frame(
                Frame.MODE_VERIFY,
                Frame.APPROVE_SCOPE_MASK,
                Optional.empty(),
                50_000L,
                0L,
                Wei.ZERO,
                Bytes.EMPTY));
    final Bytes witness = Bytes.repeat(Bytes.fromHexString(fill).get(0), 32);
    final List<FrameSignature> withWitness =
        List.of(
            new FrameSignature(
                FrameSignature.SCHEME_ARBITRARY, Bytes.EMPTY, Bytes.fromHexString(msg), witness));
    final List<FrameSignature> withoutWitness =
        List.of(
            new FrameSignature(
                FrameSignature.SCHEME_ARBITRARY,
                Bytes.EMPTY,
                Bytes.fromHexString(msg),
                Bytes.EMPTY));
    final List<UInt256> keys = KeyedNonces.LEGACY_NONCE_KEYS;

    assertThat(
            FrameTransactionGas.intrinsicGas(frames, withWitness, sender, keys, 1L)
                - FrameTransactionGas.intrinsicGas(frames, withoutWitness, sender, keys, 1L))
        .isEqualTo(expectedIntrinsicCost);
    assertThat(
            FrameTransactionGas.calldataFloorGas(frames, withWitness, sender, keys, 1L)
                - FrameTransactionGas.calldataFloorGas(frames, withoutWitness, sender, keys, 1L))
        .isEqualTo(2_048L);
  }

  @Test
  void floorCapVectorMatchesTheSpecFixture() {
    assertThat(
            TransactionEncoder.encodeOpaqueBytes(
                KeyedNonceGasCapVectors.atCapFloor(), EncodingContext.BLOCK_BODY))
        .isEqualTo(KeyedNonceGasCapVectors.atCapFloorBytes());
  }

  static Stream<Arguments> capVectors() {
    return Stream.of(
        Arguments.of(
            "at_cap-standard", KeyedNonceGasCapVectors.atCapStandard(), 16_777_216L, false),
        Arguments.of(
            "above_cap-standard", KeyedNonceGasCapVectors.aboveCapStandard(), 16_777_217L, false),
        Arguments.of("at_cap-floor", KeyedNonceGasCapVectors.atCapFloor(), 16_777_174L, true),
        Arguments.of(
            "above_cap-floor", KeyedNonceGasCapVectors.aboveCapFloor(), 16_777_238L, true));
  }

  /**
   * The bound the EIP-7825 cap applies to, max(intrinsic + execution gas, calldata floor), of the
   * spec's 16-key cap vectors, and the derived gas limit, which adds the declared state gas.
   */
  @ParameterizedTest(name = "{0}")
  @MethodSource("capVectors")
  void capBoundIncludesTheKeyedNonceAccessCost(
      final String name,
      final Transaction transaction,
      final long expectedBound,
      final boolean floorBinds) {
    final List<Frame> frames = transaction.getFrames().orElseThrow();
    final long standard =
        clampedAdd(
            FrameTransactionGas.intrinsicGas(transaction),
            FrameTransactionGas.totalExecutionGasLimit(frames));
    final long floor = FrameTransactionGas.calldataFloorGas(transaction);

    assertThat(Math.max(standard, floor)).isEqualTo(expectedBound);
    assertThat(floor > standard).isEqualTo(floorBinds);
    assertThat(transaction.getGasLimit())
        .isEqualTo(expectedBound + FrameTransactionGas.totalStateGasLimit(frames));
  }
}
