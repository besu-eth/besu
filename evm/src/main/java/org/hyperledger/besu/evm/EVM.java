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
import org.hyperledger.besu.evm.operation.MulOperationOptimized;
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
import org.hyperledger.besu.evm.operation.SubOperationOptimized;
import org.hyperledger.besu.evm.operation.SwapNOperation;
import org.hyperledger.besu.evm.operation.SwapOperation;
import org.hyperledger.besu.evm.operation.VirtualOperation;
import org.hyperledger.besu.evm.operation.XorOperation;
import org.hyperledger.besu.evm.operation.XorOperationOptimized;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.evm.v2.operation.AddModOperationV2;
import org.hyperledger.besu.evm.v2.operation.AddOperationV2;
import org.hyperledger.besu.evm.v2.operation.AndOperationV2;
import org.hyperledger.besu.evm.v2.operation.ByteOperationV2;
import org.hyperledger.besu.evm.v2.operation.CountLeadingZerosOperationV2;
import org.hyperledger.besu.evm.v2.operation.DivOperationV2;
import org.hyperledger.besu.evm.v2.operation.DupNOperationV2;
import org.hyperledger.besu.evm.v2.operation.DupOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExchangeOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExpOperationV2;
import org.hyperledger.besu.evm.v2.operation.GtOperationV2;
import org.hyperledger.besu.evm.v2.operation.InvalidOperationV2;
import org.hyperledger.besu.evm.v2.operation.IsZeroOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpDestOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpiOperationV2;
import org.hyperledger.besu.evm.v2.operation.LtOperationV2;
import org.hyperledger.besu.evm.v2.operation.ModOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulModOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulOperationV2;
import org.hyperledger.besu.evm.v2.operation.NotOperationV2;
import org.hyperledger.besu.evm.v2.operation.OperationsV2;
import org.hyperledger.besu.evm.v2.operation.OrOperationV2;
import org.hyperledger.besu.evm.v2.operation.PopOperationV2;
import org.hyperledger.besu.evm.v2.operation.PushOperationV2;
import org.hyperledger.besu.evm.v2.operation.SDivOperationV2;
import org.hyperledger.besu.evm.v2.operation.SGtOperationV2;
import org.hyperledger.besu.evm.v2.operation.SLtOperationV2;
import org.hyperledger.besu.evm.v2.operation.SModOperationV2;
import org.hyperledger.besu.evm.v2.operation.SarOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShlOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShrOperationV2;
import org.hyperledger.besu.evm.v2.operation.SignExtendOperationV2;
import org.hyperledger.besu.evm.v2.operation.StopOperationV2;
import org.hyperledger.besu.evm.v2.operation.SubOperationV2;
import org.hyperledger.besu.evm.v2.operation.SwapNOperationV2;
import org.hyperledger.besu.evm.v2.operation.SwapOperationV2;
import org.hyperledger.besu.evm.v2.operation.XorOperationV2;

import java.util.Arrays;
import java.util.Optional;
import java.util.function.Function;

import org.apache.tuweni.bytes.Bytes;
import org.jspecify.annotations.NonNull;
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

  /**
   * Hides from C2, the JVM's optimizing JIT compiler, that every case of the untraced loop ({@link
   * #runToHaltV2Untraced}) starts from the same stack top.
   *
   * <p>Stack items are 4 longs, so the operations compute offsets like {@code (top - 1) << 2}. C2
   * rewrites that as {@code (top << 2) - 4}, sees {@code top << 2} in most cases, and computes it
   * and the other shared offsets once before the switch, whichever case then runs. Those values
   * take up registers, and C2 makes room by keeping the stack top, pc and gas in memory, which
   * slows down every operation.
   *
   * <p>So the loop xors the opcode's entry into the stack top, and each case xors its own constant
   * back out. C2 can't see that the two cancel, because the entry is loaded from this array, so
   * each case's offsets stay in the case. It has to be an xor: C2 rewrites {@code (top + c) << 2}
   * as {@code (top << 2) + 4c}, so with an addition {@code top << 2} is shared again.
   */
  private static final int[] STACK_TOP_BIASES = new int[256];

  static {
    // One line per case of the untraced loop's switch, in the same order. A case's entries must
    // equal the constant the case xors back out, which is its first opcode. Opcodes without a case
    // keep 0, so the general path gets the stack top unchanged.
    biasCase(0x60, 0x60); // PUSH1
    biasCase(0x61, 0x67); // PUSH2-8
    biasCase(0x80, 0x8f); // DUP1-16
    biasCase(0x90, 0x9f); // SWAP1-16
    biasCase(0x5b, 0x5b); // JUMPDEST
    biasCase(0x50, 0x50); // POP
    biasCase(0x57, 0x57); // JUMPI
    biasCase(0x56, 0x56); // JUMP
    biasCase(0x01, 0x01); // ADD
    biasCase(0x15, 0x15); // ISZERO
    biasCase(0x16, 0x16); // AND
    biasCase(0x03, 0x03); // SUB
    biasCase(0x14, 0x14); // EQ
    biasCase(0x10, 0x10); // LT
    biasCase(0x1b, 0x1b); // SHL
    biasCase(0x11, 0x11); // GT
    biasCase(0x5f, 0x5f); // PUSH0
  }

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

  private final Operation[] operationsV2;

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

    operationsV2 =
        evmConfiguration.enableEvmV2()
            ? OperationsV2.of(operations.getOperations(), gasCalculator, evmSpecVersion)
            : null;
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
   * @param operationTracer the tracing
   */
  // Note to maintainers: lots of Java idioms and OO principals are being set aside in the
  // name of performance. This is one of the hottest sections of code.
  //
  // Please benchmark before refactoring.
  public void runToHalt(final MessageFrame frame, @NonNull final OperationTracer operationTracer) {
    if (evmConfiguration.enableEvmV2()) {
      runToHaltV2(frame, operationTracer);
      return;
    }
    evmSpecVersion.maybeWarnVersion();

    byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    Operation[] operationArray = operations.getOperations();
    // A node that builds blocks also runs this loop with the interruptible selection tracer, so the
    // tracer call sites are not monomorphic; skip them entirely when tracing is disabled.
    final boolean tracingEnabled = operationTracer.isEnabled();
    while (frame.getState() == MessageFrame.State.CODE_EXECUTING) {
      Operation currentOperation;
      int opcode;
      int pc = frame.getPC();
      if (pc < code.length) {
        opcode = code[pc] & 0xff;
        currentOperation = operationArray[opcode];
      } else {
        opcode = 0;
        currentOperation = endOfScriptStop;
      }
      frame.setCurrentOperation(currentOperation);
      if (tracingEnabled) {
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
              case 0x02 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? MulOperationOptimized.staticOperation(frame)
                      : MulOperation.staticOperation(frame);
              case 0x03 ->
                  evmConfiguration.enableOptimizedOpcodes()
                      ? SubOperationOptimized.staticOperation(frame)
                      : SubOperation.staticOperation(frame);
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
      if (tracingEnabled) {
        operationTracer.tracePostExecution(frame, result);
      }
    }
  }

  /**
   * EVM v2 execution loop using long[] stack representation. Traced execution has a loop of its
   * own, so that the untraced loop, which block import runs, has no tracer calls: they would use up
   * part of C2's inlining budget, and the first trace would make the JVM recompile the loop.
   */
  // Note: like runToHalt, this is performance-critical code. Benchmark before refactoring.
  private void runToHaltV2(final MessageFrame frame, final OperationTracer operationTracer) {
    evmSpecVersion.maybeWarnVersion();
    final boolean tracing = operationTracer.isEnabled();
    frame.setRecordsUpdatesForTracerV2(tracing);
    if (tracing) {
      runToHaltV2Traced(frame, operationTracer);
    } else {
      runToHaltV2Untraced(frame);
    }
  }

  private static void biasCase(final int firstOpcode, final int lastOpcode) {
    Arrays.fill(STACK_TOP_BIASES, firstOpcode, lastOpcode + 1, firstOpcode);
  }

  /**
   * The v2 loop for untraced execution, such as block import. It runs once per EVM operation, so it
   * is shaped around how C2 compiles it:
   *
   * <ul>
   *   <li>C2 copies (inlines) the methods a method calls into it, up to 8000 bytes of bytecode. So
   *       only the most frequent mainnet operations have a case here, and the rest go through
   *       {@link #executeOperationV2}.
   *   <li>The stack top, pc and gas are locals, because C2 can keep a local in a CPU register. The
   *       frame's fields would be written to and read from memory on every operation.
   *   <li>Two things make C2 keep a local in memory instead, which in this loop slows down every
   *       operation: a call that C2 doesn't inline and that the local is still needed after, and
   *       too many values needing a register at the same time. The comments below show where each
   *       is avoided.
   * </ul>
   */
  private void runToHaltV2Untraced(final MessageFrame frame) {
    // the loop checks the state only where an operation can change it
    if (frame.getState() != State.CODE_EXECUTING) {
      return;
    }
    final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    int sp = frame.stackTopV2();
    int pc = frame.getPC();
    long gas = frame.getRemainingGas();
    while (true) {
      final int opcode = pc < code.length ? code[pc] & 0xff : 0;
      // the stack top, disguised differently for each case; see STACK_TOP_BIASES
      final int base = sp ^ STACK_TOP_BIASES[opcode];
      final OperationResult result;
      // A switch on the opcode itself lets C2 order its compares by how often each case runs, so
      // the most frequent operations are found first.
      //
      // The operations read and write the stack top, pc and gas in the frame, so each case copies
      // the locals its operation uses into the frame and reads them back right after the call.
      // Once the operation is inlined, C2 sees each write next to its read and keeps the value in
      // the register. The read-back has to be in the case: after the switch, C2 doesn't reliably
      // pair it with the write.
      switch (opcode) {
        case 0x60 -> { // PUSH1
          frame.setTopV2(base ^ 0x60);
          frame.setPC(pc);
          result = PushOperationV2.SingleByte.staticOperation(frame, code, pc, 1);
          sp = frame.stackTopV2();
          pc = frame.getPC();
        }
        case 0x61, // PUSH2-8
            0x62,
            0x63,
            0x64,
            0x65,
            0x66,
            0x67 -> {
          frame.setTopV2(base ^ 0x61);
          frame.setPC(pc);
          result = PushOperationV2.SingleLimb.staticOperation(frame, code, pc, opcode - PUSH_BASE);
          sp = frame.stackTopV2();
          pc = frame.getPC();
        }
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
            0x8f -> {
          frame.setTopV2(base ^ 0x80);
          result = DupOperationV2.staticOperation(frame, opcode - DupOperationV2.DUP_BASE);
          sp = frame.stackTopV2();
        }
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
            0x9f -> {
          frame.setTopV2(base ^ 0x90);
          result = SwapOperationV2.staticOperation(frame, opcode - SwapOperationV2.SWAP_BASE);
          sp = frame.stackTopV2();
        }
        case 0x5b -> { // JUMPDEST
          // rebuilt from base, so that only base, not sp as well, needs a register through the
          // switch
          sp = base ^ 0x5b;
          result = JumpDestOperationV2.JUMPDEST_SUCCESS;
        }
        case 0x50 -> { // POP
          frame.setTopV2(base ^ 0x50);
          result = PopOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x57 -> { // JUMPI
          // the gas too: C2 doesn't always inline the jump destination check, and the gas local
          // must not be needed after a call that isn't inlined
          frame.setTopV2(base ^ 0x57);
          frame.setPC(pc);
          frame.setGasRemaining(gas);
          result = JumpiOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
          pc = frame.getPC();
          gas = frame.getRemainingGas();
        }
        case 0x56 -> { // JUMP
          frame.setTopV2(base ^ 0x56);
          frame.setPC(pc);
          frame.setGasRemaining(gas);
          result = JumpOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
          pc = frame.getPC();
          gas = frame.getRemainingGas();
        }
        case 0x01 -> { // ADD
          frame.setTopV2(base ^ 0x01);
          result = AddOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x15 -> { // ISZERO
          frame.setTopV2(base ^ 0x15);
          result = IsZeroOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x16 -> { // AND
          frame.setTopV2(base ^ 0x16);
          result = AndOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x03 -> { // SUB
          frame.setTopV2(base ^ 0x03);
          result = SubOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x14 -> { // EQ, whose execute checks the gas itself
          frame.setTopV2(base ^ 0x14);
          frame.setGasRemaining(gas);
          result = operationsV2[opcode].execute(frame, this);
          sp = frame.stackTopV2();
        }
        case 0x10 -> { // LT
          frame.setTopV2(base ^ 0x10);
          result = LtOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x1b -> { // SHL
          frame.setTopV2(base ^ 0x1b);
          result =
              enableConstantinople
                  ? ShlOperationV2.staticOperation(frame)
                  : InvalidOperationV2.invalidOperationResult(opcode);
          sp = frame.stackTopV2();
        }
        case 0x11 -> { // GT
          frame.setTopV2(base ^ 0x11);
          result = GtOperationV2.staticOperation(frame);
          sp = frame.stackTopV2();
        }
        case 0x5f -> { // PUSH0
          frame.setTopV2(base ^ 0x5f);
          frame.setPC(pc);
          result =
              enableShanghai
                  ? PushOperationV2.SingleByte.staticOperation(frame, code, pc, 0)
                  : InvalidOperationV2.invalidOperationResult(opcode);
          sp = frame.stackTopV2();
          pc = frame.getPC();
        }
        default -> {
          // executeOperationV2 is too big to inline, so this is a real call, and no local may be
          // needed after it. That is why this case completes the operation itself, reads all three
          // back from the frame, and has its own catch blocks. Its bias is 0, so base is the stack
          // top.
          frame.setTopV2(base);
          frame.setPC(pc);
          frame.setGasRemaining(gas);
          OperationResult generalResult;
          try {
            generalResult = executeOperationV2(frame, code, pc, opcode);
          } catch (final OverflowException oe) {
            generalResult = OVERFLOW_RESPONSE;
          } catch (final UnderflowException ue) {
            generalResult = UNDERFLOW_RESPONSE;
          }
          completeOperation(frame, generalResult);
          if (frame.getState() != State.CODE_EXECUTING) {
            return;
          }
          sp = frame.stackTopV2();
          pc = frame.getPC();
          gas = frame.getRemainingGas();
          continue;
        }
      }
      final long gasLeft = gas - result.getGasCost();
      if (result.getHaltReason() != null || gasLeft < 0) {
        // completeOperation halts the frame for either
        frame.setPC(pc);
        frame.setGasRemaining(gas);
        completeOperation(frame, result);
        return;
      }
      gas = gasLeft;
      pc += result.getPcIncrement();
    }
  }

  /**
   * The v2 interpreter loop for traced execution, which calls the tracer around every operation.
   */
  private void runToHaltV2Traced(final MessageFrame frame, final OperationTracer operationTracer) {
    final byte[] code = frame.getCode().getBytes().toArrayUnsafe();
    final Operation[] operationArray = operations.getOperations();
    while (frame.getState() == State.CODE_EXECUTING) {
      final int pc = frame.getPC();
      final int opcode = pc < code.length ? code[pc] & 0xff : 0;
      frame.setCurrentOperation(pc < code.length ? operationArray[opcode] : endOfScriptStop);
      operationTracer.tracePreExecution(frame);
      OperationResult result;
      try {
        result = executeOperationV2(frame, code, pc, opcode);
      } catch (final OverflowException oe) {
        result = OVERFLOW_RESPONSE;
      } catch (final UnderflowException ue) {
        result = UNDERFLOW_RESPONSE;
      }
      completeOperation(frame, result);
      operationTracer.tracePostExecution(frame, result);
    }
  }

  /**
   * Executes one operation on the v2 stack.
   *
   * @param frame the frame
   * @param code the code being executed
   * @param pc the program counter
   * @param opcode the operation code, 0 past the end of the code
   * @return the result of the operation
   */
  private OperationResult executeOperationV2(
      final MessageFrame frame, final byte[] code, final int pc, final int opcode) {
    return switch (opcode) {
      case 0x00 -> StopOperationV2.staticOperation(frame);
      case 0x01 -> AddOperationV2.staticOperation(frame);
      case 0x02 -> MulOperationV2.staticOperation(frame);
      case 0x03 -> SubOperationV2.staticOperation(frame);
      case 0x04 -> DivOperationV2.staticOperation(frame);
      case 0x05 -> SDivOperationV2.staticOperation(frame);
      case 0x06 -> ModOperationV2.staticOperation(frame);
      case 0x07 -> SModOperationV2.staticOperation(frame);
      case 0x08 -> AddModOperationV2.staticOperation(frame);
      case 0x09 -> MulModOperationV2.staticOperation(frame);
      case 0x0a -> ExpOperationV2.staticOperation(frame, gasCalculator);
      case 0x0b -> SignExtendOperationV2.staticOperation(frame);
      case 0x0c, 0x0d, 0x0e, 0x0f -> InvalidOperationV2.invalidOperationResult(opcode);
      case 0x10 -> LtOperationV2.staticOperation(frame);
      case 0x11 -> GtOperationV2.staticOperation(frame);
      case 0x12 -> SLtOperationV2.staticOperation(frame);
      case 0x13 -> SGtOperationV2.staticOperation(frame);
      case 0x14 -> operationsV2[opcode].execute(frame, this);
      case 0x15 -> IsZeroOperationV2.staticOperation(frame);
      case 0x16 -> AndOperationV2.staticOperation(frame);
      case 0x17 -> OrOperationV2.staticOperation(frame);
      case 0x18 -> XorOperationV2.staticOperation(frame);
      case 0x19 -> NotOperationV2.staticOperation(frame);
      case 0x1a -> ByteOperationV2.staticOperation(frame);
      case 0x1b ->
          enableConstantinople
              ? ShlOperationV2.staticOperation(frame)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0x1c ->
          enableConstantinople
              ? ShrOperationV2.staticOperation(frame)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0x1d ->
          enableConstantinople
              ? SarOperationV2.staticOperation(frame)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0x1e ->
          enableOsaka
              ? CountLeadingZerosOperationV2.staticOperation(frame)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0x50 -> PopOperationV2.staticOperation(frame);
      case 0x56 -> JumpOperationV2.staticOperation(frame);
      case 0x57 -> JumpiOperationV2.staticOperation(frame);
      case 0x5b -> JumpDestOperationV2.JUMPDEST_SUCCESS;
      case 0x5F -> // PUSH0
          enableShanghai
              ? PushOperationV2.SingleByte.staticOperation(frame, code, pc, 0)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0x60 -> // PUSH1
          PushOperationV2.SingleByte.staticOperation(frame, code, pc, 1);
      case 0x61, // PUSH2-8
          0x62,
          0x63,
          0x64,
          0x65,
          0x66,
          0x67 ->
          PushOperationV2.SingleLimb.staticOperation(frame, code, pc, opcode - PUSH_BASE);
      case 0x68, // PUSH9-32
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
          PushOperationV2.MultiLimb.staticOperation(frame, code, pc, opcode - PUSH_BASE);
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
          DupOperationV2.staticOperation(frame, opcode - DupOperationV2.DUP_BASE);
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
          SwapOperationV2.staticOperation(frame, opcode - SwapOperationV2.SWAP_BASE);
      case 0xe6 -> // DUPN (EIP-8024)
          enableAmsterdam
              ? DupNOperationV2.staticOperation(frame, code, pc)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0xe7 -> // SWAPN (EIP-8024)
          enableAmsterdam
              ? SwapNOperationV2.staticOperation(frame, code, pc)
              : InvalidOperationV2.invalidOperationResult(opcode);
      case 0xe8 -> // EXCHANGE (EIP-8024)
          enableAmsterdam
              ? ExchangeOperationV2.staticOperation(frame, code, pc)
              : InvalidOperationV2.invalidOperationResult(opcode);
      default -> operationsV2[opcode].execute(frame, this);
    };
  }

  /**
   * Charges an operation's gas and moves past it, or halts the frame if the operation failed.
   *
   * @param frame the frame
   * @param result the result of the operation
   */
  private static void completeOperation(final MessageFrame frame, final OperationResult result) {
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
    if (Hash.EMPTY.getBytes().equals(codeHash.getBytes())) {
      return Code.EMPTY_CODE;
    }

    Code result = jumpDestOnlyCodeCache.getIfPresent(codeHash);
    if (result == null) {
      result = new Code(codeBytes);
      jumpDestOnlyCodeCache.put(codeHash, result);
    }

    return result;
  }
}
