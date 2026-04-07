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
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
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
import org.hyperledger.besu.evm.v2.operation.AddressOperationV2;
import org.hyperledger.besu.evm.v2.operation.AndOperationV2;
import org.hyperledger.besu.evm.v2.operation.BalanceOperationV2;
import org.hyperledger.besu.evm.v2.operation.BaseFeeOperationV2;
import org.hyperledger.besu.evm.v2.operation.BlobBaseFeeOperationV2;
import org.hyperledger.besu.evm.v2.operation.BlobHashOperationV2;
import org.hyperledger.besu.evm.v2.operation.BlockHashOperationV2;
import org.hyperledger.besu.evm.v2.operation.ByteOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallCodeOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallDataCopyOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallDataLoadOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallDataSizeOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallValueOperationV2;
import org.hyperledger.besu.evm.v2.operation.CallerOperationV2;
import org.hyperledger.besu.evm.v2.operation.ChainIdOperationV2;
import org.hyperledger.besu.evm.v2.operation.ClzOperationV2;
import org.hyperledger.besu.evm.v2.operation.CodeCopyOperationV2;
import org.hyperledger.besu.evm.v2.operation.CodeSizeOperationV2;
import org.hyperledger.besu.evm.v2.operation.CoinbaseOperationV2;
import org.hyperledger.besu.evm.v2.operation.Create2OperationV2;
import org.hyperledger.besu.evm.v2.operation.CreateOperationV2;
import org.hyperledger.besu.evm.v2.operation.DelegateCallOperationV2;
import org.hyperledger.besu.evm.v2.operation.DifficultyOperationV2;
import org.hyperledger.besu.evm.v2.operation.DivOperationV2;
import org.hyperledger.besu.evm.v2.operation.DupNOperationV2;
import org.hyperledger.besu.evm.v2.operation.DupOperationV2;
import org.hyperledger.besu.evm.v2.operation.EqOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExchangeOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExpOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExtCodeCopyOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExtCodeHashOperationV2;
import org.hyperledger.besu.evm.v2.operation.ExtCodeSizeOperationV2;
import org.hyperledger.besu.evm.v2.operation.GasLimitOperationV2;
import org.hyperledger.besu.evm.v2.operation.GasOperationV2;
import org.hyperledger.besu.evm.v2.operation.GasPriceOperationV2;
import org.hyperledger.besu.evm.v2.operation.GtOperationV2;
import org.hyperledger.besu.evm.v2.operation.InvalidOperationV2;
import org.hyperledger.besu.evm.v2.operation.IsZeroOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpDestOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpOperationV2;
import org.hyperledger.besu.evm.v2.operation.JumpiOperationV2;
import org.hyperledger.besu.evm.v2.operation.Keccak256OperationV2;
import org.hyperledger.besu.evm.v2.operation.LogOperationV2;
import org.hyperledger.besu.evm.v2.operation.LtOperationV2;
import org.hyperledger.besu.evm.v2.operation.MCopyOperationV2;
import org.hyperledger.besu.evm.v2.operation.MSizeOperationV2;
import org.hyperledger.besu.evm.v2.operation.MloadOperationV2;
import org.hyperledger.besu.evm.v2.operation.ModOperationV2;
import org.hyperledger.besu.evm.v2.operation.Mstore8OperationV2;
import org.hyperledger.besu.evm.v2.operation.MstoreOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulModOperationV2;
import org.hyperledger.besu.evm.v2.operation.MulOperationV2;
import org.hyperledger.besu.evm.v2.operation.NotOperationV2;
import org.hyperledger.besu.evm.v2.operation.NumberOperationV2;
import org.hyperledger.besu.evm.v2.operation.OrOperationV2;
import org.hyperledger.besu.evm.v2.operation.OriginOperationV2;
import org.hyperledger.besu.evm.v2.operation.PayOperationV2;
import org.hyperledger.besu.evm.v2.operation.PcOperationV2;
import org.hyperledger.besu.evm.v2.operation.PopOperationV2;
import org.hyperledger.besu.evm.v2.operation.PrevRandaoOperationV2;
import org.hyperledger.besu.evm.v2.operation.Push0OperationV2;
import org.hyperledger.besu.evm.v2.operation.PushOperationV2;
import org.hyperledger.besu.evm.v2.operation.ReturnDataCopyOperationV2;
import org.hyperledger.besu.evm.v2.operation.ReturnDataSizeOperationV2;
import org.hyperledger.besu.evm.v2.operation.ReturnOperationV2;
import org.hyperledger.besu.evm.v2.operation.RevertOperationV2;
import org.hyperledger.besu.evm.v2.operation.SDivOperationV2;
import org.hyperledger.besu.evm.v2.operation.SLoadOperationV2;
import org.hyperledger.besu.evm.v2.operation.SModOperationV2;
import org.hyperledger.besu.evm.v2.operation.SStoreOperationV2;
import org.hyperledger.besu.evm.v2.operation.SarOperationV2;
import org.hyperledger.besu.evm.v2.operation.SelfBalanceOperationV2;
import org.hyperledger.besu.evm.v2.operation.SelfDestructOperationV2;
import org.hyperledger.besu.evm.v2.operation.SgtOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShlOperationV2;
import org.hyperledger.besu.evm.v2.operation.ShrOperationV2;
import org.hyperledger.besu.evm.v2.operation.SignExtendOperationV2;
import org.hyperledger.besu.evm.v2.operation.SlotNumOperationV2;
import org.hyperledger.besu.evm.v2.operation.SltOperationV2;
import org.hyperledger.besu.evm.v2.operation.StaticCallOperationV2;
import org.hyperledger.besu.evm.v2.operation.StopOperationV2;
import org.hyperledger.besu.evm.v2.operation.SubOperationV2;
import org.hyperledger.besu.evm.v2.operation.SwapNOperationV2;
import org.hyperledger.besu.evm.v2.operation.SwapOperationV2;
import org.hyperledger.besu.evm.v2.operation.TLoadOperationV2;
import org.hyperledger.besu.evm.v2.operation.TStoreOperationV2;
import org.hyperledger.besu.evm.v2.operation.TimestampOperationV2;
import org.hyperledger.besu.evm.v2.operation.XorOperationV2;

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
  private final boolean enableByzantium;
  private final boolean enableConstantinople;
  private final boolean enableIstanbul;
  private final boolean enableLondon;
  private final boolean enableParis;
  private final boolean enableShanghai;
  private final boolean enableCancun;
  private final boolean enableAmsterdam;
  private final boolean enableOsaka;

  // V2 operation instances that require constructor arguments
  private final ChainIdOperationV2 chainIdOperationV2;
  private final GasOperationV2 gasOperationV2;

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

    enableByzantium = EvmSpecVersion.BYZANTIUM.ordinal() <= evmSpecVersion.ordinal();
    enableConstantinople = EvmSpecVersion.CONSTANTINOPLE.ordinal() <= evmSpecVersion.ordinal();
    enableIstanbul = EvmSpecVersion.ISTANBUL.ordinal() <= evmSpecVersion.ordinal();
    enableLondon = EvmSpecVersion.LONDON.ordinal() <= evmSpecVersion.ordinal();
    enableParis = EvmSpecVersion.PARIS.ordinal() <= evmSpecVersion.ordinal();
    enableShanghai = EvmSpecVersion.SHANGHAI.ordinal() <= evmSpecVersion.ordinal();
    enableCancun = EvmSpecVersion.CANCUN.ordinal() <= evmSpecVersion.ordinal();
    enableAmsterdam = EvmSpecVersion.AMSTERDAM.ordinal() <= evmSpecVersion.ordinal();
    enableOsaka = EvmSpecVersion.OSAKA.ordinal() <= evmSpecVersion.ordinal();

    // Pre-compute V2 operation instances that require constructor arguments.
    // ChainIdOperation is only registered for Istanbul+, so the instanceof check is the gate.
    Operation chainIdOp = operations.get(ChainIdOperation.OPCODE);
    if (chainIdOp instanceof ChainIdOperation cid) {
      chainIdOperationV2 = new ChainIdOperationV2(gasCalculator, cid.getChainId());
    } else {
      chainIdOperationV2 = null;
    }
    gasOperationV2 = new GasOperationV2(gasCalculator);
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
    if (evmConfiguration.enableEvmV2()) {
      frame.ensureV2Stack();
      runToHaltV2(frame, tracing);
      return;
    }
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
      if (operationTracer != null) {
        operationTracer.tracePostExecution(frame, result);
      }
    }
  }

  /**
   * EVM v2 execution loop using long[] stack representation. Only opcodes explicitly listed in the
   * switch are handled via the v2 path; all others fall through to the v1 operation registry. This
   * skeleton stub establishes the dispatch structure for incremental v2 operation rollout.
   */
  // Note: like runToHalt, this is performance-critical code. Benchmark before refactoring.
  private void runToHaltV2(final MessageFrame frame, final OperationTracer tracing) {
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
              case 0x01 -> AddOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x02 -> MulOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x03 -> SubOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x04 -> DivOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x05 -> SDivOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x06 -> ModOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x07 -> SModOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x08 -> AddModOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x09 -> MulModOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x0a ->
                  ExpOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x0b -> SignExtendOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x10 -> LtOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x11 -> GtOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x12 -> SltOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x13 -> SgtOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x14 -> EqOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x15 -> IsZeroOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x16 -> AndOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x17 -> OrOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x18 -> XorOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x19 -> NotOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x1a -> ByteOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x1b ->
                  enableConstantinople
                      ? ShlOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1c ->
                  enableConstantinople
                      ? ShrOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1d ->
                  enableConstantinople
                      ? SarOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x1e ->
                  enableOsaka
                      ? ClzOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x50 -> PopOperationV2.staticOperation(frame);
              case 0x5f ->
                  enableShanghai
                      ? Push0OperationV2.staticOperation(frame, frame.stackDataV2())
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
                  PushOperationV2.staticOperation(
                      frame, frame.stackDataV2(), code, pc, opcode - PushOperationV2.PUSH_BASE);
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
                  DupOperationV2.staticOperation(
                      frame, frame.stackDataV2(), opcode - DupOperationV2.DUP_BASE);
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
                  SwapOperationV2.staticOperation(
                      frame, frame.stackDataV2(), opcode - SwapOperationV2.SWAP_BASE);
              case 0xe6 -> // DUPN (EIP-8024)
                  enableAmsterdam
                      ? DupNOperationV2.staticOperation(frame, frame.stackDataV2(), code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xe7 -> // SWAPN (EIP-8024)
                  enableAmsterdam
                      ? SwapNOperationV2.staticOperation(frame, frame.stackDataV2(), code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xe8 -> // EXCHANGE (EIP-8024)
                  enableAmsterdam
                      ? ExchangeOperationV2.staticOperation(frame, frame.stackDataV2(), code, pc)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x51 ->
                  MloadOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x52 ->
                  MstoreOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x53 ->
                  Mstore8OperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x5e ->
                  enableCancun
                      ? MCopyOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xf0 ->
                  CreateOperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator, this);
              case 0xf1 ->
                  CallOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator, this);
              case 0xf2 ->
                  CallCodeOperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator, this);
              case 0xf4 ->
                  DelegateCallOperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator, this);
              case 0xf5 ->
                  Create2OperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator, this);
              case 0xfa ->
                  StaticCallOperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator, this);
              case 0x54 ->
                  SLoadOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x55 ->
                  SStoreOperationV2.staticOperation(
                      frame,
                      frame.stackDataV2(),
                      gasCalculator,
                      SStoreOperationV2.EIP_1706_MINIMUM);
              case 0x5c ->
                  enableCancun
                      ? TLoadOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x5d ->
                  enableCancun
                      ? TStoreOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              // Data copy / hash / account operations
              case 0x20 ->
                  Keccak256OperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x31 ->
                  BalanceOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x35 -> CallDataLoadOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x37 ->
                  CallDataCopyOperationV2.staticOperation(
                      frame, frame.stackDataV2(), gasCalculator);
              case 0x39 ->
                  CodeCopyOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x3b ->
                  ExtCodeSizeOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x3c ->
                  ExtCodeCopyOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0x3e ->
                  enableByzantium
                      ? ReturnDataCopyOperationV2.staticOperation(
                          frame, frame.stackDataV2(), gasCalculator)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x3f ->
                  enableConstantinople
                      ? ExtCodeHashOperationV2.staticOperation(
                          frame, frame.stackDataV2(), gasCalculator)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x40 -> BlockHashOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x47 ->
                  enableIstanbul
                      ? SelfBalanceOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x49 ->
                  enableCancun
                      ? BlobHashOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              // Environment push operations (0 → 1)
              case 0x30 -> AddressOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x32 -> OriginOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x33 -> CallerOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x34 -> CallValueOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x36 -> CallDataSizeOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x38 -> CodeSizeOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x3a -> GasPriceOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x3d ->
                  enableByzantium
                      ? ReturnDataSizeOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x41 -> CoinbaseOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x42 -> TimestampOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x43 -> NumberOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x44 ->
                  enableParis
                      ? PrevRandaoOperationV2.staticOperation(frame, frame.stackDataV2())
                      : DifficultyOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x45 -> GasLimitOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x46 -> // CHAINID (Istanbul+)
                  chainIdOperationV2 != null
                      ? chainIdOperationV2.executeFixedCostOperation(frame, this)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x48 -> // BASEFEE (London+)
                  enableLondon
                      ? BaseFeeOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x4a -> // BLOBBASEFEE (Cancun+)
                  enableCancun
                      ? BlobBaseFeeOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x4b -> // SLOTNUM (Amsterdam+)
                  enableAmsterdam
                      ? SlotNumOperationV2.staticOperation(frame, frame.stackDataV2())
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0x58 -> PcOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x59 -> MSizeOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x5a -> gasOperationV2.executeFixedCostOperation(frame, this);
              // Control flow operations
              case 0x00 -> StopOperationV2.staticOperation(frame);
              case 0x56 -> JumpOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x57 -> JumpiOperationV2.staticOperation(frame, frame.stackDataV2());
              case 0x5b -> JumpDestOperationV2.staticOperation(frame);
              case 0xf3 ->
                  ReturnOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator);
              case 0xfd -> // REVERT (Byzantium+)
                  enableByzantium
                      ? RevertOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator)
                      : InvalidOperation.invalidOperationResult(opcode);
              case 0xfe -> InvalidOperationV2.INVALID_RESULT;
              case 0xa0, 0xa1, 0xa2, 0xa3, 0xa4 -> {
                int topicCount = opcode - 0xa0;
                yield LogOperationV2.staticOperation(
                    frame, frame.stackDataV2(), topicCount, gasCalculator);
              }
              case 0xff ->
                  SelfDestructOperationV2.staticOperation(
                      frame,
                      frame.stackDataV2(),
                      gasCalculator,
                      enableCancun,
                      enableAmsterdam
                          ? EIP7708TransferLogEmitter.INSTANCE
                          : TransferLogEmitter.NOOP);
              case 0xfc -> // PAY (EIP-7708, Amsterdam+)
                  enableAmsterdam
                      ? PayOperationV2.staticOperation(frame, frame.stackDataV2(), gasCalculator)
                      : InvalidOperation.invalidOperationResult(opcode);
              default -> {
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
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
      } else if (frame.decrementRemainingGas(result.getGasCost()) < 0) {
        frame.setExceptionalHaltReason(Optional.of(ExceptionalHaltReason.INSUFFICIENT_GAS));
        frame.setState(MessageFrame.State.EXCEPTIONAL_HALT);
      }
      if (frame.getState() == MessageFrame.State.CODE_EXECUTING) {
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
