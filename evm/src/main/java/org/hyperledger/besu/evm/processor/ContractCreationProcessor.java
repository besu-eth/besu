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
package org.hyperledger.besu.evm.processor;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.ModificationNotAllowedException;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.contractvalidation.ContractValidationRule;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** A contract creation message processor. */
public class ContractCreationProcessor extends AbstractMessageProcessor {

  private static final Logger LOG = LoggerFactory.getLogger(ContractCreationProcessor.class);

  private final boolean requireCodeDepositToSucceed;

  private final long initialContractNonce;

  private final List<ContractValidationRule> contractValidationRules;

  /** Strategy for emitting ETH transfer logs (no-op before Amsterdam, EIP-7708 after). */
  private final TransferLogEmitter transferLogEmitter;

  /**
   * Instantiates a new Contract creation processor.
   *
   * @param evm the evm
   * @param requireCodeDepositToSucceed the require code deposit to succeed
   * @param contractValidationRules the contract validation rules
   * @param initialContractNonce the initial contract nonce
   * @param forceCommitAddresses the force commit addresses
   */
  public ContractCreationProcessor(
      final EVM evm,
      final boolean requireCodeDepositToSucceed,
      final List<ContractValidationRule> contractValidationRules,
      final long initialContractNonce,
      final Set<Address> forceCommitAddresses) {
    this(
        evm,
        requireCodeDepositToSucceed,
        contractValidationRules,
        initialContractNonce,
        forceCommitAddresses,
        TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Contract creation processor.
   *
   * @param evm the evm
   * @param requireCodeDepositToSucceed the require code deposit to succeed
   * @param contractValidationRules the contract validation rules
   * @param initialContractNonce the initial contract nonce
   */
  public ContractCreationProcessor(
      final EVM evm,
      final boolean requireCodeDepositToSucceed,
      final List<ContractValidationRule> contractValidationRules,
      final long initialContractNonce) {
    this(
        evm,
        requireCodeDepositToSucceed,
        contractValidationRules,
        initialContractNonce,
        Collections.emptySet(),
        TransferLogEmitter.NOOP);
  }

  /**
   * Instantiates a new Contract creation processor with transfer log emission support.
   *
   * @param evm the evm
   * @param requireCodeDepositToSucceed the require code deposit to succeed
   * @param contractValidationRules the contract validation rules
   * @param initialContractNonce the initial contract nonce
   * @param forceCommitAddresses the force commit addresses
   * @param transferLogEmitter strategy for emitting transfer logs
   */
  public ContractCreationProcessor(
      final EVM evm,
      final boolean requireCodeDepositToSucceed,
      final List<ContractValidationRule> contractValidationRules,
      final long initialContractNonce,
      final Set<Address> forceCommitAddresses,
      final TransferLogEmitter transferLogEmitter) {
    super(evm, forceCommitAddresses);
    this.requireCodeDepositToSucceed = requireCodeDepositToSucceed;
    this.contractValidationRules = contractValidationRules;
    this.initialContractNonce = initialContractNonce;
    this.transferLogEmitter = transferLogEmitter;
  }

  private static boolean accountExists(final Account account) {
    // The account exists if it has sent a transaction
    // or already has its code initialized.
    return account.getNonce() != 0 || !account.getCode().isEmpty() || !account.isStorageEmpty();
  }

  @Override
  public void start(final MessageFrame frame, final OperationTracer operationTracer) {
    if (LOG.isTraceEnabled()) {
      LOG.trace("Executing contract-creation");
    }
    try {

      final MutableAccount sender = frame.getWorldUpdater().getSenderAccount(frame);
      sender.decrementBalance(frame.getValue());

      Address contractAddress = frame.getContractAddress();
      final MutableAccount contract = frame.getWorldUpdater().getOrCreate(contractAddress);
      frame.getEip7928AccessList().ifPresent(t -> t.addTouchedAccount(contractAddress));
      if (accountExists(contract)) {
        LOG.trace(
            "Contract creation error: account has already been created for address {}",
            contractAddress);
        frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE));
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
        operationTracer.traceAccountCreationResult(
            frame, Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE));
      } else {
        frame.addCreate(contractAddress);
        contract.incrementBalance(frame.getValue());

        // Emit transfer log for nonzero value contract creation (no-op before Amsterdam)
        transferLogEmitter.emitTransferLog(
            frame, frame.getSenderAddress(), contractAddress, frame.getValue());

        contract.setNonce(initialContractNonce);
        contract.clearStorage();
        frame.setState(MessageFrame.State.CODE_EXECUTING);
      }
    } catch (final ModificationNotAllowedException ex) {
      LOG.trace("Contract creation error: attempt to mutate an immutable account");
      frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.ILLEGAL_STATE_CHANGE));
      frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
    }
  }

  @Override
  public void codeSuccess(final MessageFrame frame, final OperationTracer operationTracer) {
    final Bytes contractCode =
        frame.getCreatedCode() == null ? frame.getOutputData() : frame.getCreatedCode().getBytes();

    // Oversized contracts must fail without charging code deposit gas or state gas.
    // We must check this first.
    final Optional<ExceptionalHaltReason> firstValidationFailure =
        contractValidationRules.stream()
            .map(rule -> rule.validate(contractCode, frame, evm))
            .flatMap(Optional::stream)
            .findFirst();
    if (firstValidationFailure.isPresent()) {
      if (frame.getDepth() == 0) {
        failCodeDepositWithoutRollback(frame, operationTracer, firstValidationFailure);
      } else {
        frame.setExceptionalHaltReason(firstValidationFailure);
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
        operationTracer.traceAccountCreationResult(frame, firstValidationFailure);
      }
      return;
    }

    // Check and charge code deposit gas (regular gas) before state gas
    final long depositFee = evm.getGasCalculator().codeDepositGasCost(contractCode.size());
    if (frame.getRemainingGas() < depositFee) {
      LOG.trace(
          "Not enough gas to pay the code deposit fee for {}: "
              + "remaining gas = {} < {} = deposit fee",
          frame.getContractAddress(),
          frame.getRemainingGas(),
          depositFee);
      if (requireCodeDepositToSucceed) {
        LOG.trace("Contract creation error: insufficient funds for code deposit");
        frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
        operationTracer.traceAccountCreationResult(
            frame, Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      } else {
        frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
      }
      return;
    }
    frame.decrementRemainingGas(depositFee);

    // Only now charge state gas for code deposit (cpsb * codeSize).
    if (!evm.getGasCalculator()
        .stateGasCostCalculator()
        .chargeCodeDepositStateGas(frame, contractCode.size())) {
      LOG.trace("Contract creation error: insufficient state gas for code deposit");
      if (frame.getDepth() == 0) {
        // Do NOT force-charge state gas here. The spec's charge_state_gas raises
        // OutOfGasError without modifying anything (no reservoir drain, no stateGasUsed
        // increment). failCodeDepositWithoutRollback clears remaining gas, matching
        // the spec's exception handler behavior of burning gas_left.
        failCodeDepositWithoutRollback(
            frame, operationTracer, Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      } else {
        frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
        operationTracer.traceAccountCreationResult(
            frame, Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
      }
      return;
    }

    final MutableAccount contract = frame.getWorldUpdater().getOrCreate(frame.getContractAddress());
    contract.setCode(contractCode);
    LOG.trace(
        "Successful creation of contract {} with code of size {} (Gas remaining: {})",
        frame.getContractAddress(),
        contractCode.size(),
        frame.getRemainingGas());
    frame.setState(MessageFrame.State.COMPLETED_SUCCESS);
    if (operationTracer.isExtendedTracing()) {
      operationTracer.traceAccountCreationResult(frame, Optional.empty());
    }
  }

  /**
   * Fails a depth-0 code deposit without triggering the normal EXCEPTIONAL_HALT rollback path. This
   * preserves stateGasUsed for EIP-8037 block gas accounting. The world state is still reverted and
   * all gas is cleared.
   *
   * @param frame the message frame
   * @param operationTracer the operation tracer
   * @param haltReason the exceptional halt reason to report
   */
  private void failCodeDepositWithoutRollback(
      final MessageFrame frame,
      final OperationTracer operationTracer,
      final Optional<ExceptionalHaltReason> haltReason) {
    LOG.trace(
        "Contract creation failed (no rollback): {} for address {}",
        haltReason,
        frame.getContractAddress());
    // Revert world state changes without calling frame.rollback() (which would undo stateGasUsed).
    // revert() undoes the world state mutations from this frame's execution.
    // commit() propagates the reverted (clean) state to the parent updater.
    // frame.rollback() is deliberately avoided: it would undo stateGasUsed tracking via the
    // UndoScalar mechanism, which must be preserved for EIP-8037 block gas accounting.
    frame.getWorldUpdater().revert();
    frame.getWorldUpdater().commit();
    frame.clearLogs();
    frame.clearGasRefund();
    frame.clearGasRemaining();
    frame.clearOutputData();
    // Do NOT call frame.setExceptionalHaltReason() here.
    // MainnetTransactionProcessor (processTransaction ~line 454) zeros the state gas reservoir when
    // exceptionalHaltReason is present. For depth-0 code deposit failures, the reservoir must be
    // preserved to avoid inflating block gas accounting. COMPLETED_FAILED state is sufficient to
    // signal failure. If MTP's reservoir-zeroing logic changes, this assumption must be revisited.
    frame.setState(MessageFrame.State.COMPLETED_FAILED);
    operationTracer.traceAccountCreationResult(frame, haltReason);
  }
}
