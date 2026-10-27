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
package org.hyperledger.besu.ethereum.mainnet.feemarket;

import org.hyperledger.besu.datatypes.Wei;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import java.util.Optional;
import java.util.function.Supplier;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.units.bigints.UInt256;
import org.apache.tuweni.units.bigints.UInt256s;

/**
 * The fee market of EIP-8198 (Quick Slots), where the max change of the base fee per block is
 * derived from the spacing between the block and its parent, so that the base fee reacts at the
 * same rate per unit of wall-clock time whatever the slot duration is.
 *
 * <p>The max change is {@code min(secondsElapsed, 12 * slotsElapsed) / (8 * 12 * slotsElapsed)},
 * that is 1/8 for 12-second slots, like London, and 5/48 for 10-second slots.
 */
public class BogotaFeeMarket extends BlobFeeMarket {

  /** The slot duration the max change of the base fee per block of London refers to. */
  static final long BASE_FEE_REFERENCE_SLOT_SECONDS = 12L;

  private static final UInt256 REFERENCE_SLOT_SECONDS =
      UInt256.valueOf(BASE_FEE_REFERENCE_SLOT_SECONDS);

  BogotaFeeMarket(
      final long londonForkBlockNumber,
      final Optional<Wei> baseFeePerGasOverride,
      final long baseFeeUpdateFraction) {
    super(londonForkBlockNumber, baseFeePerGasOverride, baseFeeUpdateFraction);
  }

  /**
   * {@inheritDoc}
   *
   * @throws IllegalArgumentException if the timestamp or the slot number of the block is not
   *     greater than the parent one, since the block cannot follow the parent.
   */
  @Override
  public Wei computeBaseFee(
      final long blockNumber,
      final long timestamp,
      final long slotNumber,
      final BlockHeader parentBlockHeader) {
    return computeBaseFeeFromSpacing(
        blockNumber,
        parentBlockHeader,
        elapsed(parentBlockHeader.getTimestamp(), timestamp, "timestamp"),
        elapsed(parentBlockHeader.getSlotNumber(), slotNumber, "slot number"));
  }

  /**
   * {@inheritDoc}
   *
   * <p>The next block is assumed to follow the head with the spacing per slot of the head, or 12
   * seconds per slot if the parent of the head is not available, like when the head is the genesis
   * block, or if the timestamp and slot number of the head are not greater than the parent ones.
   */
  @Override
  public Wei predictNextBaseFee(
      final BlockHeader headBlockHeader,
      final Supplier<Optional<BlockHeader>> headParentBlockHeaderSupplier) {
    final long nextBlockNumber = headBlockHeader.getNumber() + 1;
    return headParentBlockHeaderSupplier
        .get()
        .filter(headParentBlockHeader -> isSpaced(headParentBlockHeader, headBlockHeader))
        .map(
            headParentBlockHeader ->
                computeBaseFeeFromSpacing(
                    nextBlockNumber,
                    headBlockHeader,
                    elapsed(
                        headParentBlockHeader.getTimestamp(),
                        headBlockHeader.getTimestamp(),
                        "timestamp"),
                    elapsed(
                        headParentBlockHeader.getSlotNumber(),
                        headBlockHeader.getSlotNumber(),
                        "slot number")))
        .orElseGet(
            () ->
                computeBaseFeeFromSpacing(
                    nextBlockNumber, headBlockHeader, REFERENCE_SLOT_SECONDS, UInt256.ONE));
  }

  private Wei computeBaseFeeFromSpacing(
      final long blockNumber,
      final BlockHeader parentBlockHeader,
      final UInt256 secondsElapsed,
      final UInt256 slotsElapsed) {
    final UInt256 referenceSecondsElapsed = slotsElapsed.multiply(REFERENCE_SLOT_SECONDS);
    // the cap keeps the max change at 1/8 at most, whatever the gap between the timestamps is
    return computeBaseFee(
        blockNumber,
        parentBlockHeader,
        UInt256s.min(secondsElapsed, referenceSecondsElapsed),
        referenceSecondsElapsed.multiply(getBasefeeMaxChangeDenominator()));
  }

  private static boolean isSpaced(final BlockHeader parentBlockHeader, final BlockHeader header) {
    return Long.compareUnsigned(header.getTimestamp(), parentBlockHeader.getTimestamp()) > 0
        && Long.compareUnsigned(header.getSlotNumber(), parentBlockHeader.getSlotNumber()) > 0;
  }

  /**
   * The difference between two unsigned 64-bit values, as an unbounded integer, since the max
   * change computation must not overflow.
   */
  private static UInt256 elapsed(final long parentValue, final long value, final String name) {
    if (Long.compareUnsigned(value, parentValue) <= 0) {
      throw new IllegalArgumentException(
          String.format(
              "The %s %s is not greater than the parent one %s",
              name, Long.toUnsignedString(value), Long.toUnsignedString(parentValue)));
    }
    return UInt256.fromBytes(Bytes.ofUnsignedLong(value - parentValue));
  }
}
