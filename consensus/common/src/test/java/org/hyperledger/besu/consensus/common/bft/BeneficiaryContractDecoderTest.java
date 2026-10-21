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
package org.hyperledger.besu.consensus.common.bft;

import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.bft.BeneficiaryContractDecoder.WeightedBeneficiary;
import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.junit.jupiter.api.Test;

public class BeneficiaryContractDecoderTest {

  private static final Address A =
      Address.fromHexString("0x00000000000000000000000000000000000000a1");
  private static final Address B =
      Address.fromHexString("0x00000000000000000000000000000000000000b2");
  private static final Address C =
      Address.fromHexString("0x00000000000000000000000000000000000000c3");

  /** Standard ABI encoding of {@code (address[], uint256[])}, as Solidity returns it. */
  static Bytes encode(final List<Address> addresses, final List<Long> weights) {
    final List<Bytes> parts = new ArrayList<>();
    parts.add(word(64));
    parts.add(word(64 + 32 + 32L * addresses.size()));
    parts.add(word(addresses.size()));
    addresses.forEach(a -> parts.add(Bytes32.leftPad(a.getBytes())));
    parts.add(word(weights.size()));
    weights.forEach(w -> parts.add(word(w)));
    return Bytes.concatenate(parts.toArray(new Bytes[0]));
  }

  static Bytes word(final long value) {
    return Bytes32.leftPad(Bytes.wrap(BigInteger.valueOf(value).toByteArray()).trimLeadingZeros());
  }

  @Test
  public void decodesValidResultInOrder() {
    final Optional<List<WeightedBeneficiary>> result =
        BeneficiaryContractDecoder.decode(encode(List.of(A, B, C), List.of(5000L, 3000L, 2000L)));

    assertThat(result)
        .contains(
            List.of(
                new WeightedBeneficiary(A, 5000),
                new WeightedBeneficiary(B, 3000),
                new WeightedBeneficiary(C, 2000)));
  }

  @Test
  public void singleBeneficiaryWithAllTheWeightIsValid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A), List.of(10_000L))))
        .contains(List.of(new WeightedBeneficiary(A, 10_000)));
  }

  @Test
  public void duplicateAddressesAreAllowed() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A, A), List.of(4000L, 6000L))))
        .contains(List.of(new WeightedBeneficiary(A, 4000), new WeightedBeneficiary(A, 6000)));
  }

  @Test
  public void zeroWeightEntryIsValidWhenTheSumIsRight() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A, B), List.of(0L, 10_000L))))
        .contains(List.of(new WeightedBeneficiary(A, 0), new WeightedBeneficiary(B, 10_000)));
  }

  @Test
  public void maximumNumberOfBeneficiariesIsValid() {
    final int n = BeneficiaryContractDecoder.MAX_BENEFICIARIES;
    final List<Address> addresses = new ArrayList<>();
    final List<Long> weights = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      addresses.add(Address.fromHexString(String.format("0x%040x", i + 1)));
      weights.add(i == 0 ? 10_000L - (n - 1) : 1L);
    }
    assertThat(BeneficiaryContractDecoder.decode(encode(addresses, weights)))
        .hasValueSatisfying(list -> assertThat(list).hasSize(n));
  }

  @Test
  public void tooManyBeneficiariesIsInvalid() {
    final int n = BeneficiaryContractDecoder.MAX_BENEFICIARIES + 1;
    final List<Address> addresses = new ArrayList<>();
    final List<Long> weights = new ArrayList<>();
    for (int i = 0; i < n; i++) {
      addresses.add(Address.fromHexString(String.format("0x%040x", i + 1)));
      weights.add(i == 0 ? 10_000L - (n - 1) : 1L);
    }
    assertThat(BeneficiaryContractDecoder.decode(encode(addresses, weights))).isEmpty();
  }

  @Test
  public void emptyListIsInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(), List.of()))).isEmpty();
  }

  @Test
  public void mismatchedLengthsAreInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A, B), List.of(10_000L))))
        .isEmpty();
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A), List.of(5_000L, 5_000L))))
        .isEmpty();
  }

  @Test
  public void weightsSummingToLessThanTotalAreInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A, B), List.of(5000L, 4999L))))
        .isEmpty();
  }

  @Test
  public void weightsSummingToMoreThanTotalAreInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A, B), List.of(5000L, 5001L))))
        .isEmpty();
  }

  @Test
  public void aSingleWeightAboveTotalIsInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(encode(List.of(A), List.of(10_001L)))).isEmpty();
  }

  @Test
  public void weightsThatOverflowWhenSummedAreInvalid() {
    final Bytes valid = encode(List.of(A, B), List.of(1L, 1L));
    // Replace the first weight with 2^256 - 1
    final int firstWeight = 32 * 2 + 32 + 64 + 32;
    final Bytes malicious =
        Bytes.concatenate(
            valid.slice(0, firstWeight),
            Bytes32.fromHexString("0x" + "ff".repeat(32)),
            valid.slice(firstWeight + 32));
    assertThat(BeneficiaryContractDecoder.decode(malicious)).isEmpty();
  }

  @Test
  public void zeroAddressIsInvalid() {
    assertThat(
            BeneficiaryContractDecoder.decode(
                encode(List.of(Address.ZERO, A), List.of(5000L, 5000L))))
        .isEmpty();
  }

  @Test
  public void addressWithNonZeroPaddingIsInvalid() {
    final Bytes valid = encode(List.of(A), List.of(10_000L));
    final int firstAddressWord = 32 * 2 + 32;
    final byte[] bytes = valid.toArray();
    bytes[firstAddressWord] = 1;
    assertThat(BeneficiaryContractDecoder.decode(Bytes.wrap(bytes))).isEmpty();
  }

  @Test
  public void emptyAndTruncatedOutputIsInvalid() {
    assertThat(BeneficiaryContractDecoder.decode(Bytes.EMPTY)).isEmpty();
    assertThat(BeneficiaryContractDecoder.decode(Bytes.wrap(new byte[63]))).isEmpty();

    final Bytes valid = encode(List.of(A, B), List.of(5000L, 5000L));
    for (int cut = 1; cut < valid.size(); cut += 7) {
      assertThat(BeneficiaryContractDecoder.decode(valid.slice(0, valid.size() - cut)))
          .as("truncated by %s bytes", cut)
          .isEmpty();
    }
  }

  @Test
  public void hugeLengthIsInvalidWithoutAllocating() {
    final Bytes valid = encode(List.of(A), List.of(10_000L));
    // Overwrite the address array length with 2^255
    final Bytes malicious =
        Bytes.concatenate(
            valid.slice(0, 64), Bytes32.fromHexString("0x80" + "00".repeat(31)), valid.slice(96));
    assertThat(BeneficiaryContractDecoder.decode(malicious)).isEmpty();
  }

  @Test
  public void misalignedOrOutOfRangeOffsetsAreInvalid() {
    final Bytes valid = encode(List.of(A), List.of(10_000L));
    // first offset 65 (not a multiple of 32)
    assertThat(BeneficiaryContractDecoder.decode(Bytes.concatenate(word(65), valid.slice(32))))
        .isEmpty();
    // first offset pointing into the head
    assertThat(BeneficiaryContractDecoder.decode(Bytes.concatenate(word(32), valid.slice(32))))
        .isEmpty();
    // second offset beyond the end
    assertThat(
            BeneficiaryContractDecoder.decode(
                Bytes.concatenate(valid.slice(0, 32), word(1 << 20), valid.slice(64))))
        .isEmpty();
  }
}
