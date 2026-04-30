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
package org.hyperledger.besu.evm.frame;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Diagnostic trace for EIP-8037 state-gas accounting under the per-opcode metering model.
 *
 * <p>Emits one JSON line per event when the system property {@code eip8037.trace} is set to {@code
 * true}. Output goes to the file named by {@code eip8037.trace.file}, or stderr if that property is
 * unset.
 *
 * <p>When disabled (the default), every {@code Eip8037Trace.xxx(...)} call site is guarded by
 * {@code if (Eip8037Trace.ENABLED)} so the disabled path is a single field read.
 */
public final class Eip8037Trace {

  /** Whether tracing is enabled. Toggled by the {@code eip8037.trace} system property. */
  public static final boolean ENABLED = Boolean.getBoolean("eip8037.trace");

  private static final PrintStream OUT;

  static {
    PrintStream stream = System.err;
    if (ENABLED) {
      final String file = System.getProperty("eip8037.trace.file");
      if (file != null && !file.isEmpty()) {
        try {
          stream = new PrintStream(new FileOutputStream(file, false), true);
        } catch (final IOException e) {
          System.err.println("Eip8037Trace: failed to open " + file + ": " + e);
        }
      }
    }
    OUT = stream;
  }

  private Eip8037Trace() {}

  /**
   * Emit a {@code FRAME_ENTER} event when a frame begins executing.
   *
   * @param depth the frame depth
   * @param address the frame's contract address
   * @param gasLimit the frame's initial gas limit
   * @param reservoir the state-gas reservoir at frame entry
   * @param stateGasUsed the cumulative state gas used at frame entry
   */
  public static void frameEnter(
      final int depth,
      final String address,
      final long gasLimit,
      final long reservoir,
      final long stateGasUsed) {
    line(
        "FRAME_ENTER",
        "depth",
        depth,
        "addr",
        quote(address),
        "gasLimit",
        gasLimit,
        "reservoir",
        reservoir,
        "stateGasUsed",
        stateGasUsed);
  }

  /**
   * Emit a {@code FRAME_EXIT} event when the frame completes.
   *
   * @param depth the frame depth
   * @param address the frame's contract address
   * @param status one of {@code SUCCESS}, {@code REVERT}, {@code HALT}
   * @param gasLeft the gas remaining at frame exit
   * @param reservoir the state-gas reservoir at frame exit
   * @param stateGasUsed the cumulative state gas used at frame exit
   */
  public static void frameExit(
      final int depth,
      final String address,
      final String status,
      final long gasLeft,
      final long reservoir,
      final long stateGasUsed) {
    line(
        "FRAME_EXIT",
        "depth",
        depth,
        "addr",
        quote(address),
        "status",
        quote(status),
        "gasLeft",
        gasLeft,
        "reservoir",
        reservoir,
        "stateGasUsed",
        stateGasUsed);
  }

  /**
   * Emit a {@code CONSUME_STATE} event when state-gas is debited from reservoir or gasRemaining.
   *
   * @param depth the frame depth
   * @param requested the requested state-gas amount
   * @param reservoirBefore reservoir value before the debit
   * @param gasLeftBefore gasRemaining value before the debit
   * @param ok whether the debit was fully covered
   * @param reservoirAfter reservoir value after the debit
   * @param gasLeftAfter gasRemaining value after the debit
   * @param stateGasUsedAfter cumulative state gas used after the debit
   */
  public static void consumeState(
      final int depth,
      final long requested,
      final long reservoirBefore,
      final long gasLeftBefore,
      final boolean ok,
      final long reservoirAfter,
      final long gasLeftAfter,
      final long stateGasUsedAfter) {
    line(
        "CONSUME_STATE",
        "depth",
        depth,
        "requested",
        requested,
        "reservoirBefore",
        reservoirBefore,
        "gasLeftBefore",
        gasLeftBefore,
        "ok",
        ok,
        "reservoirAfter",
        reservoirAfter,
        "gasLeftAfter",
        gasLeftAfter,
        "stateGasUsedAfter",
        stateGasUsedAfter);
  }

  /**
   * Emit a {@code CREDIT_RESERVOIR} event when the reservoir is credited (refund).
   *
   * @param depth the frame depth
   * @param amount the credit amount
   * @param reservoirBefore reservoir value before the credit
   * @param reservoirAfter reservoir value after the credit
   */
  public static void creditReservoir(
      final int depth, final long amount, final long reservoirBefore, final long reservoirAfter) {
    line(
        "CREDIT_RESERVOIR",
        "depth",
        depth,
        "amount",
        amount,
        "reservoirBefore",
        reservoirBefore,
        "reservoirAfter",
        reservoirAfter);
  }

  /**
   * Emit a {@code REC_STORAGE} event when an SSTORE writes a slot.
   *
   * @param depth the frame depth
   * @param address the storage owner address
   * @param key the storage slot key
   * @param txEntryIsZero whether the original tx-entry value is zero
   * @param beforeIsZero whether the current value before this SSTORE is zero
   * @param afterIsZero whether the new value being written is zero
   */
  public static void recStorage(
      final int depth,
      final String address,
      final String key,
      final boolean txEntryIsZero,
      final boolean beforeIsZero,
      final boolean afterIsZero) {
    line(
        "REC_STORAGE",
        "depth",
        depth,
        "addr",
        quote(address),
        "key",
        quote(key),
        "txEntryIsZero",
        txEntryIsZero,
        "beforeIsZero",
        beforeIsZero,
        "afterIsZero",
        afterIsZero);
  }

  /**
   * Emit a {@code REC_ACCT_CREATED} event when a new account is materialised.
   *
   * @param depth the frame depth
   * @param address the address of the newly created account
   */
  public static void recAccountCreated(final int depth, final String address) {
    line("REC_ACCT_CREATED", "depth", depth, "addr", quote(address));
  }

  /**
   * Emit a {@code REC_CODE_DEPOSIT} event when code is deposited at an address.
   *
   * @param depth the frame depth
   * @param address the address receiving the code
   * @param codeLength the size of the deposited code in bytes
   */
  public static void recCodeDeposit(final int depth, final String address, final int codeLength) {
    line("REC_CODE_DEPOSIT", "depth", depth, "addr", quote(address), "len", codeLength);
  }

  /**
   * Emit a {@code SPILL_RESTORE} event summarising the spill/burn calculus on revert/halt. Specific
   * to the per-opcode metering model.
   *
   * @param depth the frame depth being unwound
   * @param isInitialFrame whether this is the initial (depth-0) frame
   * @param stateGasRestored stateGasUsed restored by frame rollback
   * @param reservoirRestored reservoir restored by frame rollback
   * @param noGrowthRefundsInScope no-growth refunds applied within this frame's scope
   * @param grossSpill gross spill amount before applying burn
   * @param burned amount classified as burned (excluded from parent's reservoir)
   * @param restored amount credited back to the reservoir
   */
  public static void spillRestore(
      final int depth,
      final boolean isInitialFrame,
      final long stateGasRestored,
      final long reservoirRestored,
      final long noGrowthRefundsInScope,
      final long grossSpill,
      final long burned,
      final long restored) {
    line(
        "SPILL_RESTORE",
        "depth",
        depth,
        "isInitial",
        isInitialFrame,
        "stateGasRestored",
        stateGasRestored,
        "reservoirRestored",
        reservoirRestored,
        "noGrowthRefundsInScope",
        noGrowthRefundsInScope,
        "grossSpill",
        grossSpill,
        "burned",
        burned,
        "restored",
        restored);
  }

  /**
   * Emit a {@code TX_END} event summarising the transaction-end totals.
   *
   * @param gasUsed gas used by the transaction
   * @param stateGasUsed effective state gas used
   * @param reservoir state-gas reservoir at tx end
   */
  public static void txEnd(final long gasUsed, final long stateGasUsed, final long reservoir) {
    line("TX_END", "gasUsed", gasUsed, "stateGasUsed", stateGasUsed, "reservoir", reservoir);
  }

  // -------- helpers --------

  private static String quote(final String s) {
    return "\"" + (s == null ? "" : s) + "\"";
  }

  private static void line(final String event, final Object... kv) {
    final StringBuilder sb = new StringBuilder(128);
    sb.append("{\"e\":\"").append(event).append('"');
    for (int i = 0; i < kv.length; i += 2) {
      sb.append(",\"").append(kv[i]).append("\":").append(kv[i + 1]);
    }
    sb.append('}');
    OUT.println(sb);
  }
}
