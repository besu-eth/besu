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
package org.hyperledger.besu.evm.frame;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;

/**
 * Diagnostic trace for EIP-8037 state-gas accounting. Emits one JSON line per event when the system
 * property {@code eip8037.trace} is set to {@code true}. Output goes to the file named by {@code
 * eip8037.trace.file} (default: stderr).
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
   * Emit a {@code FRAME_ENTER} event captured at frame construction.
   *
   * @param depth message-stack depth of the frame
   * @param address contract address of the frame
   * @param gasLimit gas remaining at frame entry
   * @param reservoir state-gas reservoir at frame entry
   * @param stateGasUsed tx-wide state-gas used at frame entry
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
   * Emit a {@code FRAME_EXIT} event captured when the frame completes.
   *
   * @param depth message-stack depth of the frame
   * @param address contract address of the frame
   * @param status exit status (e.g. {@code SUCCESS}, {@code REVERT}, {@code HALT}, {@code
   *     STATE_OOG})
   * @param gasLeft gas remaining at frame exit
   * @param reservoir state-gas reservoir at frame exit
   * @param stateGasUsed tx-wide state-gas used at frame exit
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
   * Emit a {@code CONSUME_STATE} event when state-gas is debited from reservoir or remaining gas.
   *
   * @param depth message-stack depth of the frame
   * @param requested amount of state-gas requested
   * @param reservoirBefore reservoir balance before the debit
   * @param gasLeftBefore gas remaining before the debit
   * @param ok whether the debit succeeded (false on OOG)
   * @param reservoirAfter reservoir balance after the debit
   * @param gasLeftAfter gas remaining after the debit
   * @param stateGasUsedAfter tx-wide state-gas used after the debit
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
   * Emit a {@code CREDIT_RESERVOIR} event when the state-gas reservoir is credited.
   *
   * @param depth message-stack depth of the frame
   * @param amount amount credited to the reservoir
   * @param reservoirBefore reservoir balance before the credit
   * @param reservoirAfter reservoir balance after the credit
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
   * Emit a {@code REC_ACCT_CREATED} event when an account-created state change is recorded.
   *
   * @param depth message-stack depth of the frame
   * @param address newly created account address
   */
  public static void recAccountCreated(final int depth, final String address) {
    line("REC_ACCT_CREATED", "depth", depth, "addr", quote(address));
  }

  /**
   * Emit a {@code REC_CODE_DEPOSIT} event when a code-deposit state change is recorded.
   *
   * @param depth message-stack depth of the frame
   * @param address account receiving the code
   * @param codeLen length of the deposited code in bytes
   */
  public static void recCodeDeposit(final int depth, final String address, final int codeLen) {
    line("REC_CODE_DEPOSIT", "depth", depth, "addr", quote(address), "len", codeLen);
  }

  /**
   * Emit a {@code REC_STORAGE} event when a storage state change is recorded.
   *
   * @param depth message-stack depth of the frame
   * @param address account whose storage changed
   * @param key storage slot key
   * @param txEntryIsZero whether the slot was zero at tx entry
   * @param beforeIsZero whether the slot was zero immediately before this write
   * @param afterIsZero whether the slot is zero immediately after this write
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
   * Emit an {@code APPLY_FRAME} event summarising frame-end state-gas accounting.
   *
   * @param depth message-stack depth of the frame
   * @param byteDiff net byte-diff for the frame
   * @param growthCost {@code byteDiff * costPerStateByte}
   * @param alreadyPaid state-gas already charged by successful descendants
   * @param thisCost signed residual ({@code growthCost - alreadyPaid})
   * @param action one of {@code CHARGE}, {@code REFUND}, {@code NOOP}
   * @param ok whether the residual charge succeeded (false on OOG)
   * @param reservoirAfter state-gas reservoir after applying the residual
   * @param gasLeftAfter gas remaining after applying the residual
   * @param stateGasUsedAfter tx-wide state-gas used after applying the residual
   */
  public static void applyFrame(
      final int depth,
      final long byteDiff,
      final long growthCost,
      final long alreadyPaid,
      final long thisCost,
      final String action,
      final boolean ok,
      final long reservoirAfter,
      final long gasLeftAfter,
      final long stateGasUsedAfter) {
    line(
        "APPLY_FRAME",
        "depth",
        depth,
        "byteDiff",
        byteDiff,
        "growthCost",
        growthCost,
        "alreadyPaid",
        alreadyPaid,
        "thisCost",
        thisCost,
        "action",
        quote(action),
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
   * Emit a {@code TX_END} event summarising the transaction-end totals.
   *
   * @param gasUsed total regular gas used by the transaction
   * @param stateGasUsed total state-gas used by the transaction
   * @param reservoir state-gas reservoir balance at tx end
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
