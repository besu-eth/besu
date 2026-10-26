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
package org.hyperledger.besu.evm.operation;

import static org.hyperledger.besu.evm.internal.Words.clampedToLong;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TransactionDiff;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;

/**
 * The EIP-7906 TXTRACE operation (0xb6): reads one value of the executing frame transaction's state
 * diff, selected by a parameter and, for the per-entry parameters, an index into the table the
 * parameter enumerates.
 *
 * <p>Stack: {@code param} on top, then {@code in2}. It costs a flat warm access (100) for every
 * parameter, warms nothing and records nothing in the block access list. It halts exceptionally
 * outside a POST_TX frame, for an undefined parameter, for a non-zero {@code in2} where the
 * parameter takes none, and for an index past the end of its table.
 */
public class TxTraceOperation extends AbstractOperation {

  /** The TXTRACE opcode. */
  public static final int OPCODE = 0xb6;

  /** Parameter: the number of balance changes. */
  public static final int BALANCES_CHANGED = 0x00;

  /** Parameter: the number of slot changes. */
  public static final int SLOTS_CHANGED = 0x01;

  /** Parameter: the number of deployed contracts. */
  public static final int CONTRACTS_DEPLOYED = 0x02;

  /** Parameter: the address of a balance change. */
  public static final int BALANCE_ADDRESS = 0x03;

  /** Parameter: the prestate balance of a balance change. */
  public static final int BALANCE_BEFORE = 0x04;

  /** Parameter: the current balance of a balance change. */
  public static final int BALANCE_AFTER = 0x05;

  /** Parameter: the address of a slot change. */
  public static final int SLOT_ADDRESS = 0x06;

  /** Parameter: the key of a slot change. */
  public static final int SLOT_KEY = 0x07;

  /** Parameter: the prestate value of a slot change. */
  public static final int SLOT_BEFORE = 0x08;

  /** Parameter: the current value of a slot change. */
  public static final int SLOT_AFTER = 0x09;

  /** Parameter: the address of a deployed contract. */
  public static final int DEPLOYED_ADDRESS = 0x0A;

  /** Parameter: the code hash of a deployed contract. */
  public static final int DEPLOYED_CODEHASH = 0x0B;

  /** Parameter: the number of events. */
  public static final int EVENTS_COUNT = 0x0C;

  /** Parameter: the emitter of an event. */
  public static final int EVENT_ADDRESS = 0x0D;

  /** Parameter: the number of topics of an event. */
  public static final int EVENT_TOPIC_COUNT = 0x0E;

  /** Parameter: the first topic of an event; 0x10 to 0x12 select the next three. */
  public static final int EVENT_TOPIC0 = 0x0F;

  /** Parameter: the last topic of an event. */
  public static final int EVENT_TOPIC3 = 0x12;

  /** Parameter: the data length of an event. */
  public static final int EVENT_DATA_LEN = 0x13;

  /** Parameter: the gas pre-charge, the maximum cost the payer was charged. */
  public static final int GAS_PRE_CHARGE = 0x14;

  /** Parameter: the payer. */
  public static final int GAS_PAYER = 0x15;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxTraceOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "TXTRACE", 2, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final long cost = gasCalculator().getWarmStorageReadCost();
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      // Not a frame transaction.
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final int param = parameter(frame.popStackItem());
    final Bytes in2 = frame.popStackItem();
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final AssertionResult result = lookup(context, param, clampedToLong(in2), in2.isZero());
    if (result.halt() != null) {
      return new OperationResult(cost, result.halt());
    }
    frame.pushStackItem(result.value());
    return new OperationResult(cost, null);
  }

  /**
   * Decodes a parameter operand of a v1 stack. Every value above 0xff is undefined for the
   * assertion operations, so it decodes as {@link Integer#MAX_VALUE}, as the v2 clamped read does.
   *
   * @param word the operand, at any width
   * @return the parameter, or {@link Integer#MAX_VALUE} when it does not fit a byte
   */
  public static int parameter(final Bytes word) {
    final Bytes trimmed = word.trimLeadingZeros();
    if (trimmed.size() > 1) {
      return Integer.MAX_VALUE;
    }
    return trimmed.isEmpty() ? 0 : Byte.toUnsignedInt(trimmed.get(0));
  }

  /**
   * Reads one TXTRACE value, after the operation charged its gas and checked that a POST_TX frame
   * executes.
   *
   * @param context the frame transaction context
   * @param param the parameter, clamped to {@link Integer#MAX_VALUE}
   * @param index the {@code in2} operand, clamped to {@link Long#MAX_VALUE}
   * @param indexIsZero whether the full 256-bit {@code in2} is zero
   * @return the value, or the halt
   */
  public static AssertionResult lookup(
      final FrameTransactionContext context,
      final int param,
      final long index,
      final boolean indexIsZero) {
    if (param == GAS_PRE_CHARGE) {
      return indexIsZero
          ? AssertionResult.of(context.maxCost().toBytes())
          : AssertionResult.INVALID;
    }
    if (param == GAS_PAYER) {
      if (!indexIsZero) {
        return AssertionResult.INVALID;
      }
      // unset only in a transaction that is invalid anyway
      final Address payer = context.payer();
      return AssertionResult.of(payer == null ? Bytes.EMPTY : payer.getBytes());
    }
    if (param < 0 || param > EVENT_DATA_LEN) {
      return AssertionResult.INVALID;
    }
    final Optional<TransactionDiff> maybeDiff = context.transactionDiff();
    if (maybeDiff.isEmpty()) {
      return AssertionResult.INVALID;
    }
    final TransactionDiff diff = maybeDiff.get();
    return switch (param) {
      case BALANCES_CHANGED -> count(indexIsZero, diff.balances());
      case SLOTS_CHANGED -> count(indexIsZero, diff.slots());
      case CONTRACTS_DEPLOYED -> count(indexIsZero, diff.deployed());
      case EVENTS_COUNT -> count(indexIsZero, diff.events());
      case BALANCE_ADDRESS, BALANCE_BEFORE, BALANCE_AFTER -> {
        if (index >= diff.balances().size()) {
          yield AssertionResult.OUT_OF_BOUNDS;
        }
        final TransactionDiff.BalanceChange change = diff.balances().get((int) index);
        yield AssertionResult.of(
            switch (param) {
              case BALANCE_ADDRESS -> change.address().getBytes();
              case BALANCE_BEFORE -> change.before().toBytes();
              default -> change.after().toBytes();
            });
      }
      case SLOT_ADDRESS, SLOT_KEY, SLOT_BEFORE, SLOT_AFTER -> {
        if (index >= diff.slots().size()) {
          yield AssertionResult.OUT_OF_BOUNDS;
        }
        final TransactionDiff.SlotChange change = diff.slots().get((int) index);
        yield AssertionResult.of(
            switch (param) {
              case SLOT_ADDRESS -> change.address().getBytes();
              case SLOT_KEY -> change.key();
              case SLOT_BEFORE -> change.before();
              default -> change.after();
            });
      }
      case DEPLOYED_ADDRESS, DEPLOYED_CODEHASH -> {
        if (index >= diff.deployed().size()) {
          yield AssertionResult.OUT_OF_BOUNDS;
        }
        final TransactionDiff.DeployedContract deployed = diff.deployed().get((int) index);
        yield AssertionResult.of(
            param == DEPLOYED_ADDRESS
                ? deployed.address().getBytes()
                : deployed.codeHash().getBytes());
      }
      default -> {
        // the event parameters, 0x0D to 0x13
        if (index >= diff.events().size()) {
          yield AssertionResult.OUT_OF_BOUNDS;
        }
        final TransactionDiff.Event event = diff.events().get((int) index);
        if (param == EVENT_ADDRESS) {
          yield AssertionResult.of(event.logger().getBytes());
        }
        if (param == EVENT_TOPIC_COUNT) {
          yield AssertionResult.of(Words.longBytes(event.topics().size()));
        }
        if (param == EVENT_DATA_LEN) {
          yield AssertionResult.of(Words.longBytes(event.data().size()));
        }
        final int position = param - EVENT_TOPIC0;
        yield position < event.topics().size()
            ? AssertionResult.of(event.topics().get(position))
            : AssertionResult.OUT_OF_BOUNDS;
      }
    };
  }

  private static AssertionResult count(final boolean indexIsZero, final List<?> table) {
    return indexIsZero
        ? AssertionResult.of(Words.longBytes(table.size()))
        : AssertionResult.INVALID;
  }
}
