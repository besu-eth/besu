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

import static org.hyperledger.besu.evm.internal.Words.clampedAdd;
import static org.hyperledger.besu.evm.internal.Words.clampedMultiply;

import org.hyperledger.besu.datatypes.Address;

import java.util.List;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-8141 gas schedule: intrinsic cost, calldata floor and maximum gas of a frame transaction.
 * All values are pure functions of the transaction payload; state-dependent costs are charged at
 * runtime inside frame budgets.
 */
public final class FrameTransactionGas {

  /** Base intrinsic cost of a frame transaction. */
  public static final long FRAME_TX_INTRINSIC_COST = 12_000L;

  /** Additional intrinsic cost per frame. */
  public static final long FRAME_TX_PER_FRAME_COST = 475L;

  /** Intrinsic verification cost of a SECP256K1 signature entry. */
  public static final long SECP256K1_SIGNATURE_GAS = 2_800L;

  /** Intrinsic verification cost of a P256 signature entry. */
  public static final long P256_SIGNATURE_GAS = 6_700L;

  /** Intrinsic cost of an ARBITRARY signature entry. */
  public static final long ARBITRARY_SIGNATURE_GAS = 100L;

  /** EIP-7976 weighted token cost applied to intrinsic calldata. */
  private static final long STANDARD_TOKEN_COST = 4L;

  /** EIP-7976 floor cost per token (a byte counts as 4 tokens on the floor). */
  private static final long TOTAL_COST_FLOOR_PER_TOKEN = 16L;

  /** EIP-2780 cost of a top-level value transfer to an address other than the sender. */
  private static final long TX_VALUE_COST = 6_000L;

  private FrameTransactionGas() {}

  /**
   * The intrinsic verification cost of a signature entry.
   *
   * @param scheme the signature scheme
   * @return the verification gas, or -1 for an unknown scheme
   */
  public static long signatureGas(final int scheme) {
    return switch (scheme) {
      case FrameSignature.SCHEME_ARBITRARY -> ARBITRARY_SIGNATURE_GAS;
      case FrameSignature.SCHEME_SECP256K1 -> SECP256K1_SIGNATURE_GAS;
      case FrameSignature.SCHEME_P256 -> P256_SIGNATURE_GAS;
      default -> -1L;
    };
  }

  private static long tokensIn(final Bytes data) {
    int zeroBytes = 0;
    for (int i = 0; i < data.size(); i++) {
      if (data.get(i) == 0) {
        zeroBytes++;
      }
    }
    final long nonZeroBytes = (long) data.size() - zeroBytes;
    return zeroBytes + nonZeroBytes * 4;
  }

  private static long calldataCost(final Bytes data) {
    return clampedMultiply(STANDARD_TOKEN_COST, tokensIn(data));
  }

  private static long floorTokensIn(final Bytes data) {
    return data.size() * 4L;
  }

  private static long valueCost(final Frame frame, final Address sender) {
    if (!frame.value().isZero()
        && frame.target().isPresent()
        && !frame.target().get().getBytes().equals(sender.getBytes())) {
      return TX_VALUE_COST;
    }
    return 0L;
  }

  /**
   * The intrinsic gas of a frame transaction in the sense of EIP-2780, charged entirely in the
   * execution dimension.
   *
   * @param frames the transaction frames
   * @param signatures the transaction signature entries
   * @param sender the declared transaction sender
   * @return the intrinsic gas (saturating)
   */
  public static long intrinsicGas(
      final List<Frame> frames, final List<FrameSignature> signatures, final Address sender) {
    long gas = FRAME_TX_INTRINSIC_COST;
    gas = clampedAdd(gas, clampedMultiply(frames.size(), FRAME_TX_PER_FRAME_COST));
    for (final Frame frame : frames) {
      gas = clampedAdd(gas, calldataCost(frame.data()));
      gas = clampedAdd(gas, valueCost(frame, sender));
    }
    for (final FrameSignature sig : signatures) {
      gas = clampedAdd(gas, calldataCost(sig.signer()));
      gas = clampedAdd(gas, calldataCost(sig.msg()));
      gas = clampedAdd(gas, calldataCost(sig.signature()));
      final long sigGas = signatureGas(sig.scheme());
      gas = clampedAdd(gas, sigGas < 0 ? Long.MAX_VALUE : sigGas);
    }
    return gas;
  }

  /**
   * The EIP-7623/EIP-7976 calldata floor of a frame transaction.
   *
   * @param frames the transaction frames
   * @param signatures the transaction signature entries
   * @param sender the declared transaction sender
   * @return the calldata floor gas (saturating)
   */
  public static long calldataFloorGas(
      final List<Frame> frames, final List<FrameSignature> signatures, final Address sender) {
    long floorTokens = 0L;
    for (final Frame frame : frames) {
      floorTokens = clampedAdd(floorTokens, floorTokensIn(frame.data()));
    }
    long gas = FRAME_TX_INTRINSIC_COST;
    gas = clampedAdd(gas, clampedMultiply(frames.size(), FRAME_TX_PER_FRAME_COST));
    for (final Frame frame : frames) {
      gas = clampedAdd(gas, valueCost(frame, sender));
    }
    for (final FrameSignature sig : signatures) {
      floorTokens = clampedAdd(floorTokens, floorTokensIn(sig.signer()));
      floorTokens = clampedAdd(floorTokens, floorTokensIn(sig.msg()));
      floorTokens = clampedAdd(floorTokens, floorTokensIn(sig.signature()));
      final long sigGas = signatureGas(sig.scheme());
      gas = clampedAdd(gas, sigGas < 0 ? Long.MAX_VALUE : sigGas);
    }
    return clampedAdd(gas, clampedMultiply(TOTAL_COST_FLOOR_PER_TOKEN, floorTokens));
  }

  /**
   * The total declared execution gas of the frames.
   *
   * @param frames the transaction frames
   * @return the sum of frame execution gas limits (saturating)
   */
  public static long totalExecutionGasLimit(final List<Frame> frames) {
    long total = 0L;
    for (final Frame frame : frames) {
      total = clampedAdd(total, frame.executionGasLimit());
    }
    return total;
  }

  /**
   * The total declared state gas of the frames.
   *
   * @param frames the transaction frames
   * @return the sum of frame state gas limits (saturating)
   */
  public static long totalStateGasLimit(final List<Frame> frames) {
    long total = 0L;
    for (final Frame frame : frames) {
      total = clampedAdd(total, frame.stateGasLimit());
    }
    return total;
  }

  /**
   * The maximum gas the payer can be charged for: {@code max(standard_gas_limit, calldata_floor_gas
   * + total_state_gas)}.
   *
   * @param frames the transaction frames
   * @param signatures the transaction signature entries
   * @param sender the declared transaction sender
   * @return the transaction's max gas (saturating)
   */
  public static long maxGas(
      final List<Frame> frames, final List<FrameSignature> signatures, final Address sender) {
    final long totalStateGas = totalStateGasLimit(frames);
    final long standardGasLimit =
        clampedAdd(
            clampedAdd(intrinsicGas(frames, signatures, sender), totalExecutionGasLimit(frames)),
            totalStateGas);
    final long floorWithStateGas =
        clampedAdd(calldataFloorGas(frames, signatures, sender), totalStateGas);
    return Math.max(standardGasLimit, floorWithStateGas);
  }
}
