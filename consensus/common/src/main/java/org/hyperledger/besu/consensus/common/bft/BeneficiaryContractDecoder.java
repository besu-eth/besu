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

import org.hyperledger.besu.datatypes.Address;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * Strict decoder and validator for the return value of {@code getBeneficiaries()}, which has the
 * ABI signature {@code (address[], uint256[])}.
 *
 * <p>It is hand-written, rather than using an ABI library, so that what counts as valid is fixed by
 * this code and cannot change with a library upgrade: every node must accept and reject exactly the
 * same return data.
 *
 * <p>Besides the ABI encoding, the decoded lists must be non-empty, have at most {@link
 * #MAX_BENEFICIARIES} entries and equal lengths. Every address must be non-zero and every weight at
 * most {@link #TOTAL_WEIGHT_BPS}, and the weights must sum to exactly {@link #TOTAL_WEIGHT_BPS}.
 * Anything else is invalid and decodes to an empty result.
 */
public final class BeneficiaryContractDecoder {

  /** The maximum number of beneficiaries a contract may return. */
  public static final int MAX_BENEFICIARIES = 64;

  /** The weights must sum to this value, in basis points. */
  public static final int TOTAL_WEIGHT_BPS = 10_000;

  private static final int WORD = 32;
  private static final int HEAD_SIZE = 2 * WORD;
  private static final int ADDRESS_PADDING = WORD - Address.SIZE;
  private static final BigInteger MAX_INT = BigInteger.valueOf(Integer.MAX_VALUE);

  /** A beneficiary and its share of the block reward. */
  public record WeightedBeneficiary(Address address, int weightBps) {}

  private BeneficiaryContractDecoder() {}

  /**
   * Decodes and validates the output of {@code getBeneficiaries()}.
   *
   * @param output the raw call output
   * @return the beneficiaries in the order the contract returned them, or empty if the output is
   *     not a valid result
   */
  public static Optional<List<WeightedBeneficiary>> decode(final Bytes output) {
    if (output.size() < HEAD_SIZE) {
      return Optional.empty();
    }
    final Optional<Integer> addressesOffset = readOffset(output, 0);
    final Optional<Integer> weightsOffset = readOffset(output, WORD);
    if (addressesOffset.isEmpty() || weightsOffset.isEmpty()) {
      return Optional.empty();
    }
    final Optional<Integer> count = readLength(output, addressesOffset.get());
    final Optional<Integer> weightCount = readLength(output, weightsOffset.get());
    if (count.isEmpty() || weightCount.isEmpty()) {
      return Optional.empty();
    }
    final int n = count.get();
    if (n == 0 || n > MAX_BENEFICIARIES || n != weightCount.get()) {
      return Optional.empty();
    }
    final int addressesStart = addressesOffset.get() + WORD;
    final int weightsStart = weightsOffset.get() + WORD;
    if (!fits(output, addressesStart, n) || !fits(output, weightsStart, n)) {
      return Optional.empty();
    }

    final List<WeightedBeneficiary> beneficiaries = new ArrayList<>(n);
    long totalWeight = 0;
    for (int i = 0; i < n; i++) {
      final Bytes addressWord = output.slice(addressesStart + i * WORD, WORD);
      if (!addressWord.slice(0, ADDRESS_PADDING).isZero()) {
        return Optional.empty();
      }
      final Address address = Address.wrap(addressWord.slice(ADDRESS_PADDING, Address.SIZE));
      if (address.equals(Address.ZERO)) {
        return Optional.empty();
      }
      final BigInteger weight = output.slice(weightsStart + i * WORD, WORD).toUnsignedBigInteger();
      if (weight.compareTo(BigInteger.valueOf(TOTAL_WEIGHT_BPS)) > 0) {
        return Optional.empty();
      }
      totalWeight += weight.intValueExact();
      beneficiaries.add(new WeightedBeneficiary(address, weight.intValueExact()));
    }
    if (totalWeight != TOTAL_WEIGHT_BPS) {
      return Optional.empty();
    }
    return Optional.of(List.copyOf(beneficiaries));
  }

  /** Reads the offset of a dynamic array: 32-byte aligned, past the head, inside the output. */
  private static Optional<Integer> readOffset(final Bytes output, final int position) {
    final BigInteger offset = output.slice(position, WORD).toUnsignedBigInteger();
    if (offset.compareTo(MAX_INT) > 0) {
      return Optional.empty();
    }
    final int value = offset.intValueExact();
    if (value < HEAD_SIZE || value % WORD != 0 || value > output.size() - WORD) {
      return Optional.empty();
    }
    return Optional.of(value);
  }

  private static Optional<Integer> readLength(final Bytes output, final int position) {
    final BigInteger length = output.slice(position, WORD).toUnsignedBigInteger();
    if (length.compareTo(MAX_INT) > 0) {
      return Optional.empty();
    }
    return Optional.of(length.intValueExact());
  }

  private static boolean fits(final Bytes output, final int start, final int words) {
    return (long) start + (long) words * WORD <= output.size();
  }
}
