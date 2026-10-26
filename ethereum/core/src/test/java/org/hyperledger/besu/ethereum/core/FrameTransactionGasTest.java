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

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.encoding.EncodingContext;
import org.hyperledger.besu.ethereum.core.encoding.TransactionDecoder;
import org.hyperledger.besu.evm.frame.KeyedNonces;

import java.util.List;
import java.util.stream.Stream;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class FrameTransactionGasTest {

  /**
   * bogota-devnet-1 block 1788, tx 91: nonce keys [0xbeef]. Its canonical receipt has gasUsed
   * 137,530 and the network reports gas (max_gas) 416,810; without the keyed nonce access cost both
   * come out 12,100 short.
   */
  private static final String KEYED_TX =
      "0x06f8bd8501a38c5b87c382beef8094be59fef26b0b6cee61e6125b265130b8fa50d597f0cd010380c782c350"
          + "83017e808080e1028094000000000000000000000000000000000000deadc782c3508302cd300180f85cf8"
          + "5a0194be59fef26b0b6cee61e6125b265130b8fa50d59780b84101e9347ef4d93ed05357f580423f6b4f46"
          + "7e076b14a53ed2660887cfaf37893fda15a86261e0927c6a2eb4c7cbb475a14c3ca7cfab4f5e2a33aeee7b"
          + "85825650d8cc843b9aca00850ba43b740080c0";

  /** bogota-devnet-1 block 1788, tx 90: the legacy key set [0]; gas (max_gas) 57,425. */
  private static final String LEGACY_TX =
      "0x06f8cb8501a38c5b87c18082029a9416f7d57da54f962220a557858b134a14fdae43abf851e601809481413f"
          + "0cf12e9b6a49b1d0439e081c577d57ffffc4821388808088000000006ad1479eca010380c4821388808080"
          + "de028094f49995b30fab79d05f1c1b54e9f9d2e1fcc812adc4827530808080f848f846018080b84101177d"
          + "11083ec49b2d969a9f20b7cca634c1a204c2fba9e5d82ca262976b56093d3ce316721346ce060595b081d9"
          + "44226f997ec30b81dad6faa405d1609b3e0a62cc84773594008504a817c80080c0";

  private static Transaction decode(final String hex) {
    return TransactionDecoder.decodeOpaqueBytes(
        Bytes.fromHexString(hex), EncodingContext.BLOCK_BODY);
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
    // Only the exact legacy set is exempt. Validation rejects a zero key alongside others before
    // any gas is priced, so this pins the helper's arithmetic rather than a reachable price.
    assertThat(FrameTransactionGas.keyedNonceAccessGas(List.of(UInt256.ZERO, UInt256.valueOf(1))))
        .isEqualTo(24_200L);
  }

  @Test
  void keyedTransactionFromTheLiveChainMatchesItsCanonicalGas() {
    final Transaction transaction = decode(KEYED_TX);
    assertThat(transaction.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x532267892da63f581e9c96b410a5ca01ad1d3317469dc4bb6b5103f7c7bff38f"));

    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(35_290L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(39_610L);
    assertThat(transaction.getGasLimit()).isEqualTo(416_810L);
  }

  @Test
  void legacyTransactionFromTheLiveChainIsUnchanged() {
    final Transaction transaction = decode(LEGACY_TX);
    assertThat(transaction.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xcf72ee14d97c821fb598301773ad2e5ed62255725264a831e7d23148bd805a4f"));

    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(17_425L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(21_217L);
    assertThat(transaction.getGasLimit()).isEqualTo(57_425L);
  }

  @Test
  void sixteenKeyTransactionPaysTheAccessCostOfEveryKey() {
    final Transaction transaction = KeyedNonceGasCapVectors.atCapStandard();
    assertThat(transaction.getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0x6702dd295536e252f97b5a6ff1058bce35f2770c3d5cfc458fe2424ba639877c"));

    // Base 12,000 + one frame 475 + 16 keys x 12,100 = 206,075, plus the 18 non-zero bytes of the
    // nonce calldata: 18 x 16 = 288 as intrinsic calldata, 18 x 64 = 1,152 under the floor.
    assertThat(FrameTransactionGas.intrinsicGas(transaction)).isEqualTo(206_363L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction)).isEqualTo(207_227L);
  }

  @Test
  void floorCapVectorMatchesTheSpecFixture() {
    assertThat(KeyedNonceGasCapVectors.atCapFloor().getHash())
        .isEqualTo(
            Hash.fromHexString(
                "0xf79b65e91c52152878959ad4705f4c4d7a7e1b63fad88eaf9abe9fd6502c9cc3"));
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
