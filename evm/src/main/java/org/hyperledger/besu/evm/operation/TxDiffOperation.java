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
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.TransactionDiff;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.Words;
import org.hyperledger.besu.evm.worldstate.WorldView;

import java.util.List;
import java.util.Optional;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;
import org.apache.tuweni.units.bigints.UInt256;

/**
 * The EIP-7906 TXDIFF operation (0xb7): one keyed lookup into the executing frame transaction's
 * state diff, selected by a parameter, an address or topic value, and a slot key, a per-address or
 * per-topic index, or zero.
 *
 * <p>Stack: {@code param} on top, then {@code in2}, then {@code in3}. The address is the low 20
 * bytes of {@code in2}; a topic is all 32 bytes of it.
 *
 * <ul>
 *   <li>The slot lookups ({@code 0x00}, {@code 0x01}) cost a cold or warm storage access for the
 *       (address, slot) pair and warm only the pair, not the address. The balance and code hash
 *       lookups ({@code 0x02} to {@code 0x05}) cost a cold or warm account access and warm the
 *       address. Either warms before the gas check, as SLOAD and BALANCE do; a halt rolls the
 *       warmth back with the call frame.
 *   <li>Those six read state, so once every check has passed they record the slot or the account in
 *       the block access list, even for an account that does not exist.
 *   <li>The views ({@code 0x06} to {@code 0x0C}) are answered from the diff alone, at the flat warm
 *       access cost, and record nothing.
 * </ul>
 *
 * <p>It halts exceptionally outside a POST_TX frame, for an undefined parameter, for a non-zero
 * {@code in3} where the parameter takes none, and for a view index past the end of its view.
 */
public class TxDiffOperation extends AbstractOperation {

  /** The TXDIFF opcode. */
  public static final int OPCODE = 0xb7;

  /** Parameter: a slot's transaction prestate value. */
  public static final int SLOT_BEFORE = 0x00;

  /** Parameter: a slot's current value. */
  public static final int SLOT_AFTER = 0x01;

  /** Parameter: an account's transaction prestate balance. */
  public static final int BALANCE_BEFORE = 0x02;

  /** Parameter: an account's current balance. */
  public static final int BALANCE_AFTER = 0x03;

  /** Parameter: an account's transaction prestate code hash. */
  public static final int CODEHASH_BEFORE = 0x04;

  /** Parameter: an account's current code hash. */
  public static final int CODEHASH_AFTER = 0x05;

  /** Parameter: the number of slot changes of an address. */
  public static final int ADDRESS_SLOTS_COUNT = 0x06;

  /** Parameter: the slot table index of one of an address's slot changes. */
  public static final int ADDRESS_SLOT_INDEX = 0x07;

  /** Parameter: the number of events an address emitted. */
  public static final int ADDRESS_EVENTS_COUNT = 0x08;

  /** Parameter: the event table index of one of an address's events. */
  public static final int ADDRESS_EVENT_INDEX = 0x09;

  /** Parameter: the account change flags of an address. */
  public static final int ACCOUNT_CHANGE_FLAGS = 0x0A;

  /** Parameter: the number of events carrying a value as topic 1, 2 or 3. */
  public static final int TOPIC_EVENTS_COUNT = 0x0B;

  /** Parameter: the event table index of one of the events carrying a topic value. */
  public static final int TOPIC_EVENT_INDEX = 0x0C;

  /**
   * Instantiates the operation.
   *
   * @param gasCalculator the gas calculator
   */
  public TxDiffOperation(final GasCalculator gasCalculator) {
    super(OPCODE, "TXDIFF", 3, 1, gasCalculator);
  }

  @Override
  public OperationResult execute(final MessageFrame frame, final EVM evm) {
    final Optional<FrameTransactionContext> maybeContext = frame.getFrameTransactionContext();
    if (maybeContext.isEmpty()) {
      // Not a frame transaction.
      return new OperationResult(
          gasCalculator().getWarmStorageReadCost(), ExceptionalHaltReason.INVALID_OPERATION);
    }
    final int param = TxTraceOperation.parameter(frame.popStackItem());
    final Bytes in2 = frame.popStackItem();
    final Bytes in3 = frame.popStackItem();
    final Address address = Words.toAddress(in2);
    final UInt256 key = UInt256.fromBytes(in3);

    final long cost = chargeAndWarm(frame, gasCalculator(), param, address, key);
    if (frame.getRemainingGas() < cost) {
      return new OperationResult(cost, ExceptionalHaltReason.INSUFFICIENT_GAS);
    }
    final FrameTransactionContext context = maybeContext.get();
    if (!context.isCurrentFramePostTx()) {
      return new OperationResult(cost, ExceptionalHaltReason.INVALID_OPERATION);
    }
    final boolean in3IsZero = in3.isZero();
    final AssertionResult result =
        isStateLookup(param)
            ? lookup(frame, context, param, address, key, in3IsZero)
            : view(context, param, address, Bytes32.leftPad(in2), clampedToLong(in3), in3IsZero);
    if (result.halt() != null) {
      return new OperationResult(cost, result.halt());
    }
    frame.pushStackItem(result.value());
    return new OperationResult(cost, null);
  }

  /**
   * Whether a parameter reads state, a slot, a balance or a code hash, rather than a view of the
   * diff.
   *
   * @param param the parameter
   * @return true for {@link #SLOT_BEFORE} to {@link #CODEHASH_AFTER}
   */
  public static boolean isStateLookup(final int param) {
    return param >= SLOT_BEFORE && param <= CODEHASH_AFTER;
  }

  /**
   * Warms what a lookup accesses and prices it, before the operation checks its gas.
   *
   * <ul>
   *   <li>{@link #SLOT_BEFORE} and {@link #SLOT_AFTER} warm the (address, slot) pair only, and cost
   *       a warm access when it was warm, a cold storage access when it was not.
   *   <li>{@link #BALANCE_BEFORE} to {@link #CODEHASH_AFTER} warm the address, and cost a warm
   *       access when it was warm or is a precompile, a cold account access when it was not.
   *   <li>Every other parameter, defined or not, costs a warm access and warms nothing.
   * </ul>
   *
   * @param frame the frame, whose warm sets the lookup updates
   * @param gasCalculator the gas calculator
   * @param param the parameter, clamped to {@link Integer#MAX_VALUE}
   * @param address the address operand
   * @param key the slot key operand
   * @return the gas cost
   */
  public static long chargeAndWarm(
      final MessageFrame frame,
      final GasCalculator gasCalculator,
      final int param,
      final Address address,
      final UInt256 key) {
    if (param == SLOT_BEFORE || param == SLOT_AFTER) {
      return frame.warmUpStorage(address, key)
          ? gasCalculator.getWarmStorageReadCost()
          : gasCalculator.getColdSloadCost();
    }
    if (isStateLookup(param)) {
      return frame.warmUpAddress(address) || gasCalculator.isPrecompile(address)
          ? gasCalculator.getWarmStorageReadCost()
          : gasCalculator.getColdAccountAccessCost();
    }
    return gasCalculator.getWarmStorageReadCost();
  }

  /**
   * Reads a slot, balance or code hash before or after the transaction, after the operation charged
   * its gas and checked that a POST_TX frame executes. Once the reserved operand check passes, the
   * read records the slot or the account in the block access list.
   *
   * <p>"Before" is the transaction prestate; "after" is the live state, the execution body's
   * result. For a key the transaction never wrote both are the live value. An account that does not
   * exist reads as balance 0 and the empty code hash. The code hash is the account's own: a
   * delegation designator is not followed.
   *
   * @param frame the frame, for the live state and the block access list
   * @param context the frame transaction context
   * @param param the parameter, {@link #SLOT_BEFORE} to {@link #CODEHASH_AFTER}
   * @param address the address operand
   * @param key the slot key operand
   * @param in3IsZero whether the full 256-bit {@code in3} is zero
   * @return the value, or the halt
   */
  public static AssertionResult lookup(
      final MessageFrame frame,
      final FrameTransactionContext context,
      final int param,
      final Address address,
      final UInt256 key,
      final boolean in3IsZero) {
    final Optional<WorldView> maybePrestate = context.transactionPrestate();
    if (maybePrestate.isEmpty()) {
      return AssertionResult.INVALID;
    }
    if (param == SLOT_BEFORE || param == SLOT_AFTER) {
      frame.getEip7928AccessList().ifPresent(list -> list.addSlotAccessForAccount(address, key));
      final Account account =
          param == SLOT_BEFORE
              ? maybePrestate.get().get(address)
              : frame.getWorldUpdater().get(address);
      return AssertionResult.of(account == null ? UInt256.ZERO : account.getStorageValue(key));
    }
    if (!in3IsZero) {
      return AssertionResult.INVALID;
    }
    frame.getEip7928AccessList().ifPresent(list -> list.addTouchedAccount(address));
    final boolean before = param == BALANCE_BEFORE || param == CODEHASH_BEFORE;
    // a missing account reads as balance 0 and the empty code hash, which EXTCODEHASH reads as 0
    final TransactionDiff.AccountFields account =
        TransactionDiff.AccountFields.of(
            before ? maybePrestate.get().get(address) : frame.getWorldUpdater().get(address));
    return AssertionResult.of(
        param == BALANCE_BEFORE || param == BALANCE_AFTER
            ? account.balance().toBytes()
            : account.codeHash().getBytes());
  }

  /**
   * Reads a per-address or per-topic view of the diff, or an account's change flags, after the
   * operation charged its gas and checked that a POST_TX frame executes. Views read nothing but the
   * diff, so they record nothing.
   *
   * @param context the frame transaction context
   * @param param the parameter, clamped to {@link Integer#MAX_VALUE}; anything that is not a view
   *     halts
   * @param address the address operand, the low 20 bytes of {@code in2}
   * @param topic the topic operand, all 32 bytes of {@code in2}
   * @param localIndex the {@code in3} operand as an index, clamped to {@link Long#MAX_VALUE}
   * @param in3IsZero whether the full 256-bit {@code in3} is zero
   * @return the value, or the halt
   */
  public static AssertionResult view(
      final FrameTransactionContext context,
      final int param,
      final Address address,
      final Bytes32 topic,
      final long localIndex,
      final boolean in3IsZero) {
    if (param < ADDRESS_SLOTS_COUNT || param > TOPIC_EVENT_INDEX) {
      return AssertionResult.INVALID;
    }
    final boolean takesIndex =
        param == ADDRESS_SLOT_INDEX || param == ADDRESS_EVENT_INDEX || param == TOPIC_EVENT_INDEX;
    if (!takesIndex && !in3IsZero) {
      return AssertionResult.INVALID;
    }
    final Optional<TransactionDiff> maybeDiff = context.transactionDiff();
    if (maybeDiff.isEmpty()) {
      return AssertionResult.INVALID;
    }
    final TransactionDiff diff = maybeDiff.get();
    return switch (param) {
      case ADDRESS_SLOTS_COUNT -> count(diff.slotIndicesOf(address));
      case ADDRESS_SLOT_INDEX -> entry(diff.slotIndicesOf(address), localIndex);
      case ADDRESS_EVENTS_COUNT -> count(diff.eventIndicesOf(address));
      case ADDRESS_EVENT_INDEX -> entry(diff.eventIndicesOf(address), localIndex);
      case ACCOUNT_CHANGE_FLAGS ->
          AssertionResult.of(Words.longBytes(diff.accountChangeFlags(address)));
      case TOPIC_EVENTS_COUNT -> count(diff.eventIndicesWithTopic(topic));
      default -> entry(diff.eventIndicesWithTopic(topic), localIndex);
    };
  }

  private static AssertionResult count(final List<Integer> view) {
    return AssertionResult.of(Words.longBytes(view.size()));
  }

  private static AssertionResult entry(final List<Integer> view, final long localIndex) {
    return localIndex < view.size()
        ? AssertionResult.of(Words.longBytes(view.get((int) localIndex)))
        : AssertionResult.OUT_OF_BOUNDS;
  }
}
