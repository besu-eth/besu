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
package org.hyperledger.besu.ethereum.core;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.evm.internal.Words.clampedAdd;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.datatypes.TransactionType;
import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.encoding.FrameTransactionDecoder;
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
import org.junit.jupiter.params.provider.MethodSource;

class FrameTransactionGasTest {

  private static final Address SENDER =
      Address.fromHexString("0x1111111111111111111111111111111111111111");
  private static final Address OTHER =
      Address.fromHexString("0x2222222222222222222222222222222222222222");

  /** rlp([0]) ++ rlp(0) = 0xc180 ++ 0x80: three non-zero bytes. */
  private static final Bytes LEGACY_NONCE_CALLDATA = Bytes.fromHexString("0xc18080");

  /** Three non-zero bytes: 3 * 4 tokens at 4 gas each. */
  private static final long LEGACY_NONCE_CALLDATA_COST = 48L;

  /** Three bytes: 3 * 4 floor tokens. */
  private static final long LEGACY_NONCE_FLOOR_TOKENS = 12L;

  private static final List<UInt256> LEGACY = KeyedNonces.LEGACY_NONCE_KEYS;

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
    // The frame transaction decoder is not registered for type 0x06 yet, so call it directly.
    return FrameTransactionDecoder.decode(Bytes.fromHexString(hex));
  }

  private static Frame frame(
      final Optional<Address> target,
      final long executionGas,
      final long stateGas,
      final Wei value,
      final Bytes data) {
    return new Frame(Frame.MODE_SENDER, 0, target, executionGas, stateGas, value, data);
  }

  private static Frame emptyFrame() {
    return frame(Optional.empty(), 0L, 0L, Wei.ZERO, Bytes.EMPTY);
  }

  @Test
  void signatureGasPerScheme() {
    assertThat(FrameTransactionGas.signatureGas(FrameSignature.SCHEME_ARBITRARY)).isEqualTo(100L);
    assertThat(FrameTransactionGas.signatureGas(FrameSignature.SCHEME_SECP256K1)).isEqualTo(2_800L);
    assertThat(FrameTransactionGas.signatureGas(FrameSignature.SCHEME_P256)).isEqualTo(6_700L);
    assertThat(FrameTransactionGas.signatureGas(3)).isEqualTo(-1L);
    assertThat(FrameTransactionGas.signatureGas(-1)).isEqualTo(-1L);
  }

  @Test
  void nonceCalldataEncodesKeysThenSequence() {
    assertThat(FrameTransactionGas.nonceCalldata(List.of(UInt256.ZERO), 0L))
        .isEqualTo(LEGACY_NONCE_CALLDATA);
    assertThat(FrameTransactionGas.nonceCalldata(List.of(UInt256.ONE, UInt256.valueOf(2)), 5L))
        .isEqualTo(Bytes.fromHexString("0xc2010205"));
  }

  @Test
  void gasOfATransactionUsesItsKeysAndSequence() {
    final List<UInt256> keys = List.of(UInt256.ONE, UInt256.valueOf(2));
    final Transaction transaction =
        Transaction.builder()
            .type(TransactionType.FRAME)
            .chainId(BigInteger.ONE)
            .nonce(5)
            .sender(SENDER)
            .nonceKeys(keys)
            .frames(List.of(emptyFrame()))
            .maxPriorityFeePerGas(Wei.ONE)
            .maxFeePerGas(Wei.ONE)
            .build();
    // The nonce calldata 0xc2010205 is four non-zero bytes: 64 gas as intrinsic calldata, 16
    // tokens under the floor. Each of the two keys pays the keyed nonce access cost.
    assertThat(FrameTransactionGas.intrinsicGas(transaction))
        .isEqualTo(
            FrameTransactionGas.intrinsicGas(List.of(emptyFrame()), List.of(), SENDER, keys, 5L))
        .isEqualTo(12_000L + 64L + 475L + 2 * 12_100L);
    assertThat(FrameTransactionGas.calldataFloorGas(transaction))
        .isEqualTo(
            FrameTransactionGas.calldataFloorGas(
                List.of(emptyFrame()), List.of(), SENDER, keys, 5L))
        .isEqualTo(12_000L + 475L + 2 * 12_100L + 16L * 16L);
  }

  @Test
  void intrinsicGasOfASingleEmptyFrame() {
    assertThat(
            FrameTransactionGas.intrinsicGas(List.of(emptyFrame()), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + LEGACY_NONCE_CALLDATA_COST + 475L);
  }

  @Test
  void intrinsicGasChargesFrameDataAsTokens() {
    // One zero byte (1 token) and one non-zero byte (4 tokens), at 4 gas per token.
    final Frame withData = frame(Optional.empty(), 0L, 0L, Wei.ZERO, Bytes.fromHexString("0x00ff"));
    assertThat(FrameTransactionGas.intrinsicGas(List.of(withData), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + LEGACY_NONCE_CALLDATA_COST + 475L + 20L);
  }

  @Test
  void valueCostAppliesOnlyToAnotherTarget() {
    final long base = 12_000L + LEGACY_NONCE_CALLDATA_COST + 475L;
    final Frame toOther = frame(Optional.of(OTHER), 0L, 0L, Wei.ONE, Bytes.EMPTY);
    final Frame toSender = frame(Optional.of(SENDER), 0L, 0L, Wei.ONE, Bytes.EMPTY);
    final Frame noTarget = frame(Optional.empty(), 0L, 0L, Wei.ONE, Bytes.EMPTY);
    final Frame zeroValue = frame(Optional.of(OTHER), 0L, 0L, Wei.ZERO, Bytes.EMPTY);

    assertThat(FrameTransactionGas.intrinsicGas(List.of(toOther), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(base + 6_000L);
    assertThat(FrameTransactionGas.intrinsicGas(List.of(toSender), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(base);
    assertThat(FrameTransactionGas.intrinsicGas(List.of(noTarget), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(base);
    assertThat(FrameTransactionGas.intrinsicGas(List.of(zeroValue), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(base);
  }

  @Test
  void intrinsicGasChargesEachFrame() {
    assertThat(
            FrameTransactionGas.intrinsicGas(
                List.of(emptyFrame(), emptyFrame(), emptyFrame()), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + LEGACY_NONCE_CALLDATA_COST + 3 * 475L);
  }

  @Test
  void intrinsicGasChargesSignatureBytesAndVerification() {
    // 65 non-zero signature bytes at 16 gas each, plus the secp256k1 verification cost.
    final FrameSignature secp256k1 =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 7, 65));
    assertThat(
            FrameTransactionGas.intrinsicGas(
                List.of(emptyFrame()), List.of(secp256k1), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + LEGACY_NONCE_CALLDATA_COST + 475L + 65 * 16L + 2_800L);

    // The signer and msg fields are charged as calldata too.
    final FrameSignature arbitrary =
        new FrameSignature(
            FrameSignature.SCHEME_ARBITRARY,
            OTHER.getBytes(),
            Bytes.repeat((byte) 0, 32),
            Bytes.of(1));
    assertThat(
            FrameTransactionGas.intrinsicGas(
                List.of(emptyFrame()), List.of(arbitrary), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + LEGACY_NONCE_CALLDATA_COST + 475L + 20 * 16L + 32 * 4L + 16L + 100L);
  }

  @Test
  void anUnknownSignatureSchemeSaturatesTheIntrinsicGas() {
    final FrameSignature unknown = new FrameSignature(7, Bytes.EMPTY, Bytes.EMPTY, Bytes.EMPTY);
    assertThat(
            FrameTransactionGas.intrinsicGas(
                List.of(emptyFrame()), List.of(unknown), SENDER, LEGACY, 0L))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(
            FrameTransactionGas.calldataFloorGas(
                List.of(emptyFrame()), List.of(unknown), SENDER, LEGACY, 0L))
        .isEqualTo(Long.MAX_VALUE);
    assertThat(
            FrameTransactionGas.maxGas(List.of(emptyFrame()), List.of(unknown), SENDER, LEGACY, 0L))
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void calldataFloorPricesEveryByteAtTheFloorRate() {
    // Floor: 4 tokens per byte, zero or not, at 16 gas per token, on top of the fixed costs.
    final Frame withData = frame(Optional.empty(), 0L, 0L, Wei.ZERO, Bytes.fromHexString("0x00ff"));
    assertThat(
            FrameTransactionGas.calldataFloorGas(List.of(withData), List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + 475L + 16L * (LEGACY_NONCE_FLOOR_TOKENS + 2 * 4L));

    final FrameSignature secp256k1 =
        new FrameSignature(
            FrameSignature.SCHEME_SECP256K1, Bytes.EMPTY, Bytes.EMPTY, Bytes.repeat((byte) 7, 65));
    final Frame toOther = frame(Optional.of(OTHER), 0L, 0L, Wei.ONE, Bytes.EMPTY);
    assertThat(
            FrameTransactionGas.calldataFloorGas(
                List.of(toOther), List.of(secp256k1), SENDER, LEGACY, 0L))
        .isEqualTo(12_000L + 475L + 6_000L + 2_800L + 16L * (LEGACY_NONCE_FLOOR_TOKENS + 65 * 4L));
  }

  @Test
  void totalGasLimitsSumTheFrames() {
    final List<Frame> frames =
        List.of(
            frame(Optional.empty(), 100L, 7L, Wei.ZERO, Bytes.EMPTY),
            frame(Optional.empty(), 250L, 11L, Wei.ZERO, Bytes.EMPTY));
    assertThat(FrameTransactionGas.totalExecutionGasLimit(frames)).isEqualTo(350L);
    assertThat(FrameTransactionGas.totalStateGasLimit(frames)).isEqualTo(18L);
    assertThat(FrameTransactionGas.totalExecutionGasLimit(List.of())).isZero();
  }

  @Test
  void totalGasLimitsSaturate() {
    final List<Frame> frames =
        List.of(
            frame(Optional.empty(), Long.MAX_VALUE, Long.MAX_VALUE, Wei.ZERO, Bytes.EMPTY),
            frame(Optional.empty(), 1L, 1L, Wei.ZERO, Bytes.EMPTY));
    assertThat(FrameTransactionGas.totalExecutionGasLimit(frames)).isEqualTo(Long.MAX_VALUE);
    assertThat(FrameTransactionGas.totalStateGasLimit(frames)).isEqualTo(Long.MAX_VALUE);
    assertThat(FrameTransactionGas.maxGas(frames, List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(Long.MAX_VALUE);
  }

  @Test
  void maxGasIsTheStandardLimitWhenExecutionDominates() {
    final List<Frame> frames =
        List.of(frame(Optional.empty(), 50_000L, 10_000L, Wei.ZERO, Bytes.EMPTY));
    final long intrinsic = FrameTransactionGas.intrinsicGas(frames, List.of(), SENDER, LEGACY, 0L);
    assertThat(FrameTransactionGas.maxGas(frames, List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(intrinsic + 50_000L + 10_000L);
  }

  @Test
  void maxGasIsTheFloorPlusStateGasWhenCalldataDominates() {
    // 1,000 zero bytes cost 4,000 gas of intrinsic calldata but 64,000 on the floor.
    final List<Frame> frames =
        List.of(frame(Optional.empty(), 0L, 10_000L, Wei.ZERO, Bytes.repeat((byte) 0, 1_000)));
    final long floor = FrameTransactionGas.calldataFloorGas(frames, List.of(), SENDER, LEGACY, 0L);
    assertThat(floor)
        .isGreaterThan(FrameTransactionGas.intrinsicGas(frames, List.of(), SENDER, LEGACY, 0L));
    assertThat(FrameTransactionGas.maxGas(frames, List.of(), SENDER, LEGACY, 0L))
        .isEqualTo(floor + 10_000L);
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
