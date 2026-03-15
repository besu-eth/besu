/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.evm;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.hyperledger.besu.evm.operation.PushOperation.PUSH_BASE;
import static org.hyperledger.besu.evm.operation.SwapOperation.SWAP_BASE;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.evm.frame.ExceptionalHaltReason;
import org.hyperledger.besu.evm.frame.MessageFrame;
import org.hyperledger.besu.evm.frame.MessageFrame.State;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.internal.JumpDestOnlyCodeCache;
import org.hyperledger.besu.evm.internal.OverflowException;
import org.hyperledger.besu.evm.internal.UnderflowException;
import org.hyperledger.besu.evm.operation.AddModOperation;
import org.hyperledger.besu.evm.operation.AddModOperationOptimized;
import org.hyperledger.besu.evm.operation.AddOperation;
import org.hyperledger.besu.evm.operation.AddOperationOptimized;
import org.hyperledger.besu.evm.operation.AndOperation;
import org.hyperledger.besu.evm.operation.AndOperationOptimized;
import org.hyperledger.besu.evm.operation.ByteOperation;
import org.hyperledger.besu.evm.operation.ChainIdOperation;
import org.hyperledger.besu.evm.operation.CountLeadingZerosOperation;
import org.hyperledger.besu.evm.operation.DivOperation;
import org.hyperledger.besu.evm.operation.DivOperationOptimized;
import org.hyperledger.besu.evm.operation.DupNOperation;
import org.hyperledger.besu.evm.operation.DupOperation;
import org.hyperledger.besu.evm.operation.ExchangeOperation;
import org.hyperledger.besu.evm.operation.ExpOperation;
import org.hyperledger.besu.evm.operation.GtOperation;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.IsZeroOperation;
import org.hyperledger.besu.evm.operation.JumpDestOperation;
import org.hyperledger.besu.evm.operation.JumpOperation;
import org.hyperledger.besu.evm.operation.JumpiOperation;
import org.hyperledger.besu.evm.operation.LtOperation;
import org.hyperledger.besu.evm.operation.ModOperation;
import org.hyperledger.besu.evm.operation.ModOperationOptimized;
import org.hyperledger.besu.evm.operation.MulModOperation;
import org.hyperledger.besu.evm.operation.MulModOperationOptimized;
import org.hyperledger.besu.evm.operation.MulOperation;
import org.hyperledger.besu.evm.operation.NotOperation;
import org.hyperledger.besu.evm.operation.NotOperationOptimized;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.Operation.OperationResult;
import org.hyperledger.besu.evm.operation.OperationRegistry;
import org.hyperledger.besu.evm.operation.OrOperation;
import org.hyperledger.besu.evm.operation.OrOperationOptimized;
import org.hyperledger.besu.evm.operation.PopOperation;
import org.hyperledger.besu.evm.operation.Push0Operation;
import org.hyperledger.besu.evm.operation.PushOperation;
import org.hyperledger.besu.evm.operation.SDivOperation;
import org.hyperledger.besu.evm.operation.SDivOperationOptimized;
import org.hyperledger.besu.evm.operation.SGtOperation;
import org.hyperledger.besu.evm.operation.SLtOperation;
import org.hyperledger.besu.evm.operation.SModOperation;
import org.hyperledger.besu.evm.operation.SModOperationOptimized;
import org.hyperledger.besu.evm.operation.SarOperation;
import org.hyperledger.besu.evm.operation.SarOperationOptimized;
import org.hyperledger.besu.evm.operation.ShlOperation;
import org.hyperledger.besu.evm.operation.ShlOperationOptimized;
import org.hyperledger.besu.evm.operation.ShrOperation;
import org.hyperledger.besu.evm.operation.ShrOperationOptimized;
import org.hyperledger.besu.evm.operation.SignExtendOperation;
import org.hyperledger.besu.evm.operation.StopOperation;
import org.hyperledger.besu.evm.operation.SubOperation;
import org.hyperledger.besu.evm.operation.SwapNOperation;
import org.hyperledger.besu.evm.operation.SwapOperation;
import org.hyperledger.besu.evm.operation.VirtualOperation;
import org.hyperledger.besu.evm.operation.XorOperation;
import org.hyperledger.besu.evm.operation.XorOperationOptimized;
import org.hyperledger.besu.evm.tracing.OperationTracer;

import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** The Evm. */
public class EVM {
  private static final Logger LOG = LoggerFactory.getLogger(EVM.class);

  /** The constant OVERFLOW_RESPONSE. */
  protected static final OperationResult OVERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.TOO_MANY_STACK_ITEMS);

  /** The constant UNDERFLOW_RESPONSE. */
  protected static final OperationResult UNDERFLOW_RESPONSE =
      new OperationResult(0L, ExceptionalHaltReason.INSUFFICIENT_STACK_ITEMS);

  private final OperationRegistry operations;
  private final GasCalculator gasCalculator;
  private final Operation endOfScriptStop;
  private final EvmConfiguration evmConfiguration;
  private final EvmSpecVersion evmSpecVersion;

  // Optimized operation flags
  private final boolean enableConstantinople;
  private final boolean enableShanghai;
  private final boolean enableAmsterdam;
  private final boolean enableOsaka;

  private final JumpDestOnlyCodeCache jumpDestOnlyCodeCache;

  /**
   * Instantiates a new Evm.
   *
   * @param operations the operations
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @param evmSpecVersion the evm spec version
   */
  public EVM(
      final OperationRegistry operations,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration,
      final EvmSpecVersion evmSpecVersion) {
    this.operations = operations;
    this.gasCalculator = gasCalculator;
    this.endOfScriptStop = new VirtualOperation(new StopOperation(gasCalculator));
    this.evmConfiguration = evmConfiguration;
    this.evmSpecVersion = evmSpecVersion;
    this.jumpDestOnlyCodeCache = new JumpDestOnlyCodeCache(evmConfiguration);

    enableConstantinople = EvmSpecVersion.CONSTANTINOPLE.ordinal() <= evmSpecVersion.ordinal();
    enableShanghai = EvmSpecVersion.SHANGHAI.ordinal() <= evmSpecVersion.ordinal();
    enableAmsterdam = EvmSpecVersion.AMSTERDAM.ordinal() <= evmSpecVersion.ordinal();
    enableOsaka = EvmSpecVersion.OSAKA.ordinal() <= evmSpecVersion.ordinal();
  }

  /**
   * Gets gas calculator.
   *
   * @return the gas calculator
   */
  public GasCalculator getGasCalculator() {
    return gasCalculator;
  }

  /**
   * Gets the max code size, taking configuration and version into account
   *
   * @return The max code size override, if not set the max code size for the EVM version.
   */
  public int getMaxCodeSize() {
    return evmConfiguration.maxCodeSizeOverride().orElse(evmSpecVersion.maxCodeSize);
  }

  /**
   * Gets the max initcode Size, taking configuration and version into account
   *
   * @return The max initcode size override, if not set the max initcode size for the EVM version.
   */
  public int getMaxInitcodeSize() {
    return evmConfiguration.maxInitcodeSizeOverride().orElse(evmSpecVersion.maxInitcodeSize);
  }

  /**
   * Returns the non-fork related configuration parameters of the EVM.
   *
   * @return the EVM configuration.
   */
  public EvmConfiguration getEvmConfiguration() {
    return evmConfiguration;
  }

  /**
   * Returns the configured EVM spec version for this EVM
   *
   * @return the evm spec version
   */
  public EvmSpecVersion getEvmVersion() {
    return evmSpecVersion;
  }

  /**
   * Return the ChainId this Executor is using, or empty if the EVM version does not expose chain
   * ID.
   *
   * @return the ChainId, or empty if not exposed.
   */
  public Optional<Bytes> getChainId() {
    Operation op = operations.get(ChainIdOperation.OPCODE);
    if (op instanceof ChainIdOperation chainIdOperation) {
      return Optional.of(chainIdOperation.getChainId());
    } else {
      return Optional.empty();
    }
  }

  /**
   * Run to halt.
   *
   * @param frame the frame
   * @param tracing the tracing
   */
  // Note to maintainers: lots of Java idioms and OO principals are being set aside in the
  // name of performance. This is one of the hottest sections of code.
  //
  // Please benchmark before refactoring.
  public void runToHalt(final MessageFrame frame, final OperationTracer tracing) {
    evmSpecVersion.maybeWarnVersion();

    var operationTracer = tracing == OperationTracer.NO_TRACING ? null : tracing;
    byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    Operation[] operationArray = operations.getOperations();
    while (frame.getState() == MessageFrame.State.CODE_EXECUTING) {
      Operation currentOperation;
      int opcode;
      int pc = frame.getPC();
      try {
        opcode = code[pc] & 0xff;
        currentOperation = operationArray[opcode];
      } catch (ArrayIndexOutOfBoundsException aiiobe) {
        opcode = 0;
        currentOperation = endOfScriptStop;
      }
      frame.setCurrentOperation(currentOperation);
      if (operationTracer != null) {
        operationTracer.tracePreExecution(frame);
      }

      OperationResult result;
      try {
        result =
            switch (opcode) {
              case 0x00 -> StopOperation.staticOperation(frame);
              case 0x01 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? AddOperationOptimized.staticOperation(frame)
                      : AddOperation.staticOperation(frame);
              case 0x02 -> MulOperation.staticOperation(frame);
              case 0x03 -> SubOperation.staticOperation(frame);
              case 0x04 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? DivOperationOptimized.staticOperation(frame)
                      : DivOperation.staticOperation(frame);
              case 0x05 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? SDivOperationOptimized.staticOperation(frame)
                      : SDivOperation.staticOperation(frame);
              case 0x06 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? ModOperationOptimized.staticOperation(frame)
                      : ModOperation.staticOperation(frame);
              case 0x07 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? SModOperationOptimized.staticOperation(frame)
                      : SModOperation.staticOperation(frame);
              case 0x08 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? AddModOperationOptimized.staticOperation(frame)
                      : AddModOperation.staticOperation(frame);
              case 0x09 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? MulModOperationOptimized.staticOperation(frame)
                      : MulModOperation.staticOperation(frame);
              case 0x0a -> ExpOperation.staticOperation(frame, gasCalculator);
              case 0x0b -> SignExtendOperation.staticOperation(frame);
              case 0x0c, 0x0d, 0x0e, 0x0f -> InvalidOperation.invalidOperationResult(opcode);
              case 0x10 -> LtOperation.staticOperation(frame);
              case 0x11 -> GtOperation.staticOperation(frame);
              case 0x12 -> SLtOperation.staticOperation(frame);
              case 0x13 -> SGtOperation.staticOperation(frame);
              case 0x15 -> IsZeroOperation.staticOperation(frame);
              case 0x16 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? AndOperationOptimized.staticOperation(frame)
                      : AndOperation.staticOperation(frame);
              case 0x17 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? OrOperationOptimized.staticOperation(frame)
                      : OrOperation.staticOperation(frame);
              case 0x18 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? XorOperationOptimized.staticOperation(frame)
                      : XorOperation.staticOperation(frame);
              case 0x19 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? NotOperationOptimized.staticOperation(frame)
                      : NotOperation.staticOperation(frame);
              case 0x1a -> ByteOperation.staticOperation(frame);
              case 0x1b ->
                  enableConstantinople
                      ? shiftOperation(
                          frame,
                          ShlOperation::staticOperation,
                          ShlOperationOptimized::staticOperation)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1c ->
                  enableConstantinople
                      ? shiftOperation(
                          frame,
                          ShrOperation::staticOperation,
                          ShrOperationOptimized::staticOperation)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1d ->
                  enableConstantinople
                      ? shiftOperation(
                          frame,
                          SarOperation::staticOperation,
                          SarOperationOptimized::staticOperation)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1e ->
                  enableOsaka
                      ? CountLeadingZerosOperation.staticOperation(frame)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x50 -> PopOperation.staticOperation(frame);
              case 0x56 -> JumpOperation.staticOperation(frame);
              case 0x57 -> JumpiOperation.staticOperation(frame);
              case 0x5b -> JumpDestOperation.JUMPDEST_SUCCESS;
              case 0x5f ->
                  enableShanghai
                      ? Push0Operation.staticOperation(frame)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x60, // PUSH1-32
                  0x61,
                  0x62,
                  0x63,
                  0x64,
                  0x65,
                  0x66,
                  0x67,
                  0x68,
                  0x69,
                  0x6a,
                  0x6b,
                  0x6c,
                  0x6d,
                  0x6e,
                  0x6f,
                  0x70,
                  0x71,
                  0x72,
                  0x73,
                  0x74,
                  0x75,
                  0x76,
                  0x77,
                  0x78,
                  0x79,
                  0x7a,
                  0x7b,
                  0x7c,
                  0x7d,
                  0x7e,
                  0x7f ->
                  PushOperation.staticOperation(frame, code, pc, opcode - PUSH_BASE);
              case 0x80, // DUP1-16
                  0x81,
                  0x82,
                  0x83,
                  0x84,
                  0x85,
                  0x86,
                  0x87,
                  0x88,
                  0x89,
                  0x8a,
                  0x8b,
                  0x8c,
                  0x8d,
                  0x8e,
                  0x8f ->
                  DupOperation.staticOperation(frame, opcode - DupOperation.DUP_BASE);
              case 0x90, // SWAP1-16
                  0x91,
                  0x92,
                  0x93,
                  0x94,
                  0x95,
                  0x96,
                  0x97,
                  0x98,
                  0x99,
                  0x9a,
                  0x9b,
                  0x9c,
                  0x9d,
                  0x9e,
                  0x9f ->
                  SwapOperation.staticOperation(frame, opcode - SWAP_BASE);
              case 0xe6 -> // DUPN (EIP-8024)
                  enableAmsterdam
                      ? DupNOperation.staticOperation(frame, code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xe7 -> // SWAPN (EIP-8024)
                  enableAmsterdam
                      ? SwapNOperation.staticOperation(frame, code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xe8 -> // EXCHANGE (EIP-8024)
                  enableAmsterdam
                      ? ExchangeOperation.staticOperation(frame, code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              default -> { // unoptimized operations
                frame.setCurrentOperation(currentOperation);
                yield currentOperation.execute(frame, this);
              }
            };
      } catch (final OverflowException oe) {
        result = OVERFLOW_RESPONSE;
      } catch (final UnderflowException ue) {
        result = UNDERFLOW_RESPONSE;
      }
      final ExceptionalHaltReason haltReason = result.getHaltReason();
      if (haltReason != null) {
        LOG.trace("MessageFrame evaluation halted because of {}", haltReason);
        frame.setExceptionalHaltReason(Optional.of(haltReason));
        frame.setState(State.EXCEPTIONAL_HALT);
      } else if (frame.decrementRemainingGas(result.getGasCost()) < 0) {
        frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
        frame.setState(State.EXCEPTIONAL_HALT);
      }
      if (frame.getState() == State.CODE_EXECUTING) {
        final int currentPC = frame.getPC();
        final int opSize = result.getPcIncrement();
        frame.setPC(currentPC + opSize);
      }
      if (operationTracer != null) {
        operationTracer.tracePostExecution(frame, result);
      }
    }
  }

  /**
   * Get Operations (unsafe)
   *
   * @return Operations array
   */
  public Operation[] getOperationsUnsafe() {
    return operations.getOperations();
  }

  private OperationResult shiftOperation(
      final MessageFrame frame,
      final Function<MessageFrame, OperationResult> standard,
      final Function<MessageFrame, OperationResult> optimized) {
    return evmConfiguration.enableOptimizedOpcodes()
        ? optimized.apply(frame)
        : standard.apply(frame);
  }

  /**
   * Gets or creates code instance with a cached jump destination.
   *
   * @param codeHash the code hash
   * @param codeBytes the code bytes
   * @return the code instance with the cached jump destination
   */
  public Code getOrCreateCachedJumpDest(final Hash codeHash, final Bytes codeBytes) {
    checkNotNull(codeHash);

    Code result = jumpDestOnlyCodeCache.getIfPresent(codeHash);
    if (result == null) {
      result = new Code(codeBytes);
      jumpDestOnlyCodeCache.put(codeHash, result);
    }

    return result;
  }
}
