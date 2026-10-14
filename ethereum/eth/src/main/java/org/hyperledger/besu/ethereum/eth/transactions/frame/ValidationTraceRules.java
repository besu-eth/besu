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
package org.hyperledger.besu.ethereum.eth.transactions.frame;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.mainnet.MainnetTransactionValidator;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.frame.FrameTransactionContext;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/**
 * Enforces the EIP-8141 public-mempool validation trace rules while a validation prefix is
 * simulated. The first violation is recorded; execution is not interrupted, and the caller rejects
 * the transaction afterwards.
 *
 * <p>Every frame in the prefix is held to the generic rules. The EIP exempts a {@code pay} frame
 * whose target runs the canonical paymaster code, but no canonical implementation is pinned yet, so
 * such frames are treated as ordinary code.
 */
public final class ValidationTraceRules implements OperationTracer {

  /** The canonical expiry verifier runtime code, the only code allowed to read TIMESTAMP. */
  static final Bytes EXPIRY_VERIFIER_CODE =
      Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd");

  private static final Bytes DELEGATION_PREFIX = Bytes.fromHexString("0xef0100");

  private static final Set<Integer> BANNED =
      Set.of(
          0x31, // BALANCE
          0x3A, // GASPRICE
          0x40, // BLOCKHASH
          0x41, // COINBASE
          0x43, // NUMBER
          0x44, // PREVRANDAO
          0x45, // GASLIMIT
          0x47, // SELFBALANCE
          0x48, // BASEFEE
          0x4A, // BLOBBASEFEE
          0x4B, // SLOTNUM
          0xFE, // INVALID
          0xFF); // SELFDESTRUCT

  private static final Set<Integer> CALLS = Set.of(0xF1, 0xF2, 0xF4, 0xFA);
  private static final Set<Integer> CALLS_WITH_VALUE = Set.of(0xF1, 0xF2);
  private static final Set<Integer> EXTCODE = Set.of(0x3B, 0x3C, 0x3F);
  private static final Set<Integer> CREATES = Set.of(0xF0, 0xF5, 0xF6);

  private static final int TIMESTAMP = 0x42;
  private static final int GAS = 0x5A;
  private static final int SLOAD = 0x54;
  private static final int SSTORE = 0x55;

  private final Address sender;
  private final ValidationPrefix prefix;
  private final PrecompileContractRegistry precompiles;
  private String violation;

  /**
   * Creates the rules for one transaction.
   *
   * @param sender the transaction sender
   * @param prefix the recognized validation prefix
   * @param precompiles the precompiles of the fork the transaction is simulated in
   */
  public ValidationTraceRules(
      final Address sender,
      final ValidationPrefix prefix,
      final PrecompileContractRegistry precompiles) {
    this.sender = sender;
    this.prefix = prefix;
    this.precompiles = precompiles;
  }

  /**
   * The first rule the simulation broke.
   *
   * @return the violation, if any
   */
  public Optional<String> violation() {
    return Optional.ofNullable(violation);
  }

  @Override
  public void tracePreExecution(final MessageFrame frame) {
    if (violation != null) {
      return;
    }
    final Optional<FrameTransactionContext> context = frame.getFrameTransactionContext();
    final Operation operation = frame.getCurrentOperation();
    if (context.isEmpty() || operation == null) {
      return;
    }
    final int frameIndex = context.get().currentFrameIndex();
    if (frameIndex >= prefix.length()) {
      return;
    }
    final boolean inDeploy = frameIndex == prefix.deployFrame();
    final int opcode = operation.getOpcode();
    final Address storageOwner = frame.getRecipientAddress();

    if (BANNED.contains(opcode)) {
      fail(frameIndex, operation.getName() + " is banned in the validation prefix");
    } else if (opcode == TIMESTAMP && !isCanonicalExpiryVerifier(frame)) {
      fail(frameIndex, "TIMESTAMP outside the canonical expiry verifier");
    } else if (opcode == GAS && !nextIsCall(frame)) {
      fail(frameIndex, "GAS not immediately followed by a call");
    } else if (CREATES.contains(opcode) && !inDeploy) {
      fail(frameIndex, operation.getName() + " outside the deploy frame");
    } else if (opcode == SSTORE && !(inDeploy && storageOwner.equals(sender))) {
      fail(frameIndex, "SSTORE outside the sender's storage in the deploy frame");
    } else if (opcode == SLOAD && !storageOwner.equals(sender)) {
      fail(frameIndex, "SLOAD of storage outside the sender");
    } else if (CALLS.contains(opcode)) {
      if (CALLS_WITH_VALUE.contains(opcode) && !stackWord(frame, 2).isZero()) {
        fail(frameIndex, operation.getName() + " transfers value");
      } else {
        checkTarget(frame, frameIndex, stackAddress(frame, 1));
      }
    } else if (EXTCODE.contains(opcode)) {
      checkTarget(frame, frameIndex, stackAddress(frame, 0));
    }
  }

  @Override
  public void tracePostExecution(
      final MessageFrame frame, final Operation.OperationResult operationResult) {
    // A deploy frame may create code only at the sender.
    final Operation operation = frame.getCurrentOperation();
    if (violation != null || operation == null || operationResult.getHaltReason() != null) {
      return;
    }
    final int opcode = operation.getOpcode();
    if ((opcode == 0xF0 || opcode == 0xF5) && frame.stackSize() > 0) {
      final Bytes created = frame.getStackItem(0);
      if (!created.isZero() && !Address.wrap(Bytes32.leftPad(created).slice(12)).equals(sender)) {
        frame
            .getFrameTransactionContext()
            .ifPresent(
                c -> fail(c.currentFrameIndex(), "deploy frame created code off the sender"));
      }
    }
  }

  private void checkTarget(final MessageFrame frame, final int frameIndex, final Address target) {
    if (target.equals(sender) || precompiles.get(target) != null) {
      return;
    }
    final Account account = frame.getWorldUpdater().get(target);
    if (account == null || account.getCode().isEmpty()) {
      fail(frameIndex, "call or code read of " + target + ", which has no code");
    } else if (account.getCode().size() >= 3
        && account.getCode().slice(0, 3).equals(DELEGATION_PREFIX)) {
      fail(frameIndex, "call or code read of " + target + ", which is EIP-7702 delegated");
    }
  }

  private static boolean isCanonicalExpiryVerifier(final MessageFrame frame) {
    return frame.getRecipientAddress().equals(MainnetTransactionValidator.EXPIRY_VERIFIER)
        && frame.getCode().getBytes().equals(EXPIRY_VERIFIER_CODE);
  }

  private static boolean nextIsCall(final MessageFrame frame) {
    final Bytes code = frame.getCode().getBytes();
    final int next = frame.getPC() + 1;
    return next < code.size() && CALLS.contains(Byte.toUnsignedInt(code.get(next)));
  }

  private static Bytes stackWord(final MessageFrame frame, final int offset) {
    return frame.stackSize() > offset ? frame.getStackItem(offset) : Bytes.EMPTY;
  }

  private static Address stackAddress(final MessageFrame frame, final int offset) {
    return Address.wrap(Bytes32.leftPad(stackWord(frame, offset)).slice(12));
  }

  private void fail(final int frameIndex, final String reason) {
    if (violation == null) {
      violation = "frame " + frameIndex + ": " + reason;
    }
  }
}
