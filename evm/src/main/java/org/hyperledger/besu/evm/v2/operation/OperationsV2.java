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
package org.hyperledger.besu.evm.v2.operation;

import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
import org.hyperledger.besu.evm.log.TransferLogEmitter;
import org.hyperledger.besu.evm.operation.AddModOperation;
import org.hyperledger.besu.evm.operation.AddModOperationOptimized;
import org.hyperledger.besu.evm.operation.AddOperation;
import org.hyperledger.besu.evm.operation.AddOperationOptimized;
import org.hyperledger.besu.evm.operation.AddressOperation;
import org.hyperledger.besu.evm.operation.AndOperation;
import org.hyperledger.besu.evm.operation.AndOperationOptimized;
import org.hyperledger.besu.evm.operation.BalanceOperation;
import org.hyperledger.besu.evm.operation.BaseFeeOperation;
import org.hyperledger.besu.evm.operation.BlobBaseFeeOperation;
import org.hyperledger.besu.evm.operation.BlobHashOperation;
import org.hyperledger.besu.evm.operation.BlockHashOperation;
import org.hyperledger.besu.evm.operation.ByteOperation;
import org.hyperledger.besu.evm.operation.CallCodeOperation;
import org.hyperledger.besu.evm.operation.CallDataCopyOperation;
import org.hyperledger.besu.evm.operation.CallDataLoadOperation;
import org.hyperledger.besu.evm.operation.CallDataSizeOperation;
import org.hyperledger.besu.evm.operation.CallOperation;
import org.hyperledger.besu.evm.operation.CallValueOperation;
import org.hyperledger.besu.evm.operation.CallerOperation;
import org.hyperledger.besu.evm.operation.ChainIdOperation;
import org.hyperledger.besu.evm.operation.CodeCopyOperation;
import org.hyperledger.besu.evm.operation.CodeSizeOperation;
import org.hyperledger.besu.evm.operation.CoinbaseOperation;
import org.hyperledger.besu.evm.operation.CountLeadingZerosOperation;
import org.hyperledger.besu.evm.operation.Create2Operation;
import org.hyperledger.besu.evm.operation.CreateOperation;
import org.hyperledger.besu.evm.operation.DelegateCallOperation;
import org.hyperledger.besu.evm.operation.DifficultyOperation;
import org.hyperledger.besu.evm.operation.DivOperation;
import org.hyperledger.besu.evm.operation.DivOperationOptimized;
import org.hyperledger.besu.evm.operation.DupNOperation;
import org.hyperledger.besu.evm.operation.DupOperation;
import org.hyperledger.besu.evm.operation.EqOperation;
import org.hyperledger.besu.evm.operation.ExchangeOperation;
import org.hyperledger.besu.evm.operation.ExpOperation;
import org.hyperledger.besu.evm.operation.ExtCodeCopyOperation;
import org.hyperledger.besu.evm.operation.ExtCodeHashOperation;
import org.hyperledger.besu.evm.operation.ExtCodeSizeOperation;
import org.hyperledger.besu.evm.operation.GasLimitOperation;
import org.hyperledger.besu.evm.operation.GasOperation;
import org.hyperledger.besu.evm.operation.GasPriceOperation;
import org.hyperledger.besu.evm.operation.GtOperation;
import org.hyperledger.besu.evm.operation.InvalidOperation;
import org.hyperledger.besu.evm.operation.IsZeroOperation;
import org.hyperledger.besu.evm.operation.JumpDestOperation;
import org.hyperledger.besu.evm.operation.JumpOperation;
import org.hyperledger.besu.evm.operation.JumpiOperation;
import org.hyperledger.besu.evm.operation.Keccak256Operation;
import org.hyperledger.besu.evm.operation.LogOperation;
import org.hyperledger.besu.evm.operation.LtOperation;
import org.hyperledger.besu.evm.operation.MCopyOperation;
import org.hyperledger.besu.evm.operation.MLoadOperation;
import org.hyperledger.besu.evm.operation.MSizeOperation;
import org.hyperledger.besu.evm.operation.MStore8Operation;
import org.hyperledger.besu.evm.operation.MStoreOperation;
import org.hyperledger.besu.evm.operation.ModOperation;
import org.hyperledger.besu.evm.operation.ModOperationOptimized;
import org.hyperledger.besu.evm.operation.MulModOperation;
import org.hyperledger.besu.evm.operation.MulModOperationOptimized;
import org.hyperledger.besu.evm.operation.MulOperation;
import org.hyperledger.besu.evm.operation.MulOperationOptimized;
import org.hyperledger.besu.evm.operation.NotOperation;
import org.hyperledger.besu.evm.operation.NotOperationOptimized;
import org.hyperledger.besu.evm.operation.NumberOperation;
import org.hyperledger.besu.evm.operation.Operation;
import org.hyperledger.besu.evm.operation.OrOperation;
import org.hyperledger.besu.evm.operation.OrOperationOptimized;
import org.hyperledger.besu.evm.operation.OriginOperation;
import org.hyperledger.besu.evm.operation.PCOperation;
import org.hyperledger.besu.evm.operation.PayOperation;
import org.hyperledger.besu.evm.operation.PopOperation;
import org.hyperledger.besu.evm.operation.PrevRanDaoOperation;
import org.hyperledger.besu.evm.operation.Push0Operation;
import org.hyperledger.besu.evm.operation.PushOperation;
import org.hyperledger.besu.evm.operation.ReturnDataCopyOperation;
import org.hyperledger.besu.evm.operation.ReturnDataSizeOperation;
import org.hyperledger.besu.evm.operation.ReturnOperation;
import org.hyperledger.besu.evm.operation.RevertOperation;
import org.hyperledger.besu.evm.operation.SDivOperation;
import org.hyperledger.besu.evm.operation.SDivOperationOptimized;
import org.hyperledger.besu.evm.operation.SGtOperation;
import org.hyperledger.besu.evm.operation.SLoadOperation;
import org.hyperledger.besu.evm.operation.SLtOperation;
import org.hyperledger.besu.evm.operation.SModOperation;
import org.hyperledger.besu.evm.operation.SModOperationOptimized;
import org.hyperledger.besu.evm.operation.SStoreOperation;
import org.hyperledger.besu.evm.operation.SarOperation;
import org.hyperledger.besu.evm.operation.SarOperationOptimized;
import org.hyperledger.besu.evm.operation.SelfBalanceOperation;
import org.hyperledger.besu.evm.operation.SelfDestructOperation;
import org.hyperledger.besu.evm.operation.ShlOperation;
import org.hyperledger.besu.evm.operation.ShlOperationOptimized;
import org.hyperledger.besu.evm.operation.ShrOperation;
import org.hyperledger.besu.evm.operation.ShrOperationOptimized;
import org.hyperledger.besu.evm.operation.SignExtendOperation;
import org.hyperledger.besu.evm.operation.SlotNumOperation;
import org.hyperledger.besu.evm.operation.StaticCallOperation;
import org.hyperledger.besu.evm.operation.StopOperation;
import org.hyperledger.besu.evm.operation.SubOperation;
import org.hyperledger.besu.evm.operation.SubOperationOptimized;
import org.hyperledger.besu.evm.operation.SwapNOperation;
import org.hyperledger.besu.evm.operation.SwapOperation;
import org.hyperledger.besu.evm.operation.TLoadOperation;
import org.hyperledger.besu.evm.operation.TStoreOperation;
import org.hyperledger.besu.evm.operation.TimestampOperation;
import org.hyperledger.besu.evm.operation.XorOperation;
import org.hyperledger.besu.evm.operation.XorOperationOptimized;

import org.apache.tuweni.bytes.Bytes32;

/**
 * The EVM v2 operations standing in for the operations a fork registers.
 *
 * <p>Building them from the fork's operations keeps v2 on the opcodes the fork enables and on the
 * parameters it configures them with, such as the chain id or the SSTORE minimum gas.
 */
public final class OperationsV2 {

  private OperationsV2() {}

  /**
   * Creates the v2 operation for every opcode of a fork.
   *
   * @param operations the operations the fork registers, indexed by opcode
   * @param gasCalculator the gas calculator of the fork
   * @param evmSpecVersion the fork
   * @return the v2 operations, indexed by opcode; null where the interpreter loop executes the
   *     opcode itself, or where the fork registers no operation
   * @throws IllegalArgumentException if the fork registers an operation without a v2 version
   */
  public static Operation[] of(
      final Operation[] operations,
      final GasCalculator gasCalculator,
      final EvmSpecVersion evmSpecVersion) {
    final Operation[] operationsV2 = new Operation[256];
    for (int opcode = 0; opcode < operationsV2.length; opcode++) {
      operationsV2[opcode] = v2Version(operations[opcode], gasCalculator, evmSpecVersion);
    }
    return operationsV2;
  }

  private static Operation v2Version(
      final Operation operation,
      final GasCalculator gasCalculator,
      final EvmSpecVersion evmSpecVersion) {
    return switch (operation) {
      case null -> null;
      // stack-less, so the v1 operation runs on a v2 frame as it is
      case InvalidOperation invalid -> invalid;
      // executed by the interpreter loop itself
      case StopOperation ignored -> null;
      case AddOperation ignored -> null;
      case AddOperationOptimized ignored -> null;
      case MulOperation ignored -> null;
      case MulOperationOptimized ignored -> null;
      case SubOperation ignored -> null;
      case SubOperationOptimized ignored -> null;
      case DivOperation ignored -> null;
      case DivOperationOptimized ignored -> null;
      case SDivOperation ignored -> null;
      case SDivOperationOptimized ignored -> null;
      case ModOperation ignored -> null;
      case ModOperationOptimized ignored -> null;
      case SModOperation ignored -> null;
      case SModOperationOptimized ignored -> null;
      case AddModOperation ignored -> null;
      case AddModOperationOptimized ignored -> null;
      case MulModOperation ignored -> null;
      case MulModOperationOptimized ignored -> null;
      case ExpOperation ignored -> null;
      case SignExtendOperation ignored -> null;
      case LtOperation ignored -> null;
      case GtOperation ignored -> null;
      case SLtOperation ignored -> null;
      case SGtOperation ignored -> null;
      case IsZeroOperation ignored -> null;
      case AndOperation ignored -> null;
      case AndOperationOptimized ignored -> null;
      case OrOperation ignored -> null;
      case OrOperationOptimized ignored -> null;
      case XorOperation ignored -> null;
      case XorOperationOptimized ignored -> null;
      case NotOperation ignored -> null;
      case NotOperationOptimized ignored -> null;
      case ByteOperation ignored -> null;
      case ShlOperation ignored -> null;
      case ShlOperationOptimized ignored -> null;
      case ShrOperation ignored -> null;
      case ShrOperationOptimized ignored -> null;
      case SarOperation ignored -> null;
      case SarOperationOptimized ignored -> null;
      case CountLeadingZerosOperation ignored -> null;
      case PopOperation ignored -> null;
      case JumpOperation ignored -> null;
      case JumpiOperation ignored -> null;
      case JumpDestOperation ignored -> null;
      case Push0Operation ignored -> null;
      case PushOperation ignored -> null;
      case DupOperation ignored -> null;
      case SwapOperation ignored -> null;
      case DupNOperation ignored -> null;
      case SwapNOperation ignored -> null;
      case ExchangeOperation ignored -> null;
      // executed through this table
      case EqOperation ignored -> new EqOperationV2(gasCalculator);
      case Keccak256Operation ignored -> new Keccak256OperationV2(gasCalculator);
      case AddressOperation ignored -> new AddressOperationV2(gasCalculator);
      case BalanceOperation ignored -> new BalanceOperationV2(gasCalculator);
      case OriginOperation ignored -> new OriginOperationV2(gasCalculator);
      case CallerOperation ignored -> new CallerOperationV2(gasCalculator);
      case CallValueOperation ignored -> new CallValueOperationV2(gasCalculator);
      case CallDataLoadOperation ignored -> new CallDataLoadOperationV2(gasCalculator);
      case CallDataSizeOperation ignored -> new CallDataSizeOperationV2(gasCalculator);
      case CallDataCopyOperation ignored -> new CallDataCopyOperationV2(gasCalculator);
      case CodeSizeOperation ignored -> new CodeSizeOperationV2(gasCalculator);
      case CodeCopyOperation ignored -> new CodeCopyOperationV2(gasCalculator);
      case GasPriceOperation ignored -> new GasPriceOperationV2(gasCalculator);
      case ExtCodeSizeOperation ignored -> new ExtCodeSizeOperationV2(gasCalculator);
      case ExtCodeCopyOperation ignored -> new ExtCodeCopyOperationV2(gasCalculator);
      case ReturnDataSizeOperation ignored -> new ReturnDataSizeOperationV2(gasCalculator);
      case ReturnDataCopyOperation ignored -> new ReturnDataCopyOperationV2(gasCalculator);
      case ExtCodeHashOperation ignored -> new ExtCodeHashOperationV2(gasCalculator);
      case BlockHashOperation ignored -> new BlockHashOperationV2(gasCalculator);
      case CoinbaseOperation ignored -> new CoinbaseOperationV2(gasCalculator);
      case TimestampOperation ignored -> new TimestampOperationV2(gasCalculator);
      case NumberOperation ignored -> new NumberOperationV2(gasCalculator);
      case DifficultyOperation ignored -> new DifficultyOperationV2(gasCalculator);
      case PrevRanDaoOperation ignored -> new PrevRanDaoOperationV2(gasCalculator);
      case GasLimitOperation ignored -> new GasLimitOperationV2(gasCalculator);
      case ChainIdOperation chainId ->
          new ChainIdOperationV2(gasCalculator, Bytes32.leftPad(chainId.getChainId()));
      case SelfBalanceOperation ignored -> new SelfBalanceOperationV2(gasCalculator);
      case BaseFeeOperation ignored -> new BaseFeeOperationV2(gasCalculator);
      case BlobHashOperation ignored -> new BlobHashOperationV2(gasCalculator);
      case BlobBaseFeeOperation ignored -> new BlobBaseFeeOperationV2(gasCalculator);
      case SlotNumOperation ignored -> new SlotNumOperationV2(gasCalculator);
      case MLoadOperation ignored -> new MLoadOperationV2(gasCalculator);
      case MStoreOperation ignored -> new MStoreOperationV2(gasCalculator);
      case MStore8Operation ignored -> new MStore8OperationV2(gasCalculator);
      case SLoadOperation ignored -> new SLoadOperationV2(gasCalculator);
      case SStoreOperation sStore ->
          new SStoreOperationV2(gasCalculator, sStore.getMinimumGasRemaining());
      case PCOperation ignored -> new PCOperationV2(gasCalculator);
      case MSizeOperation ignored -> new MSizeOperationV2(gasCalculator);
      case GasOperation ignored -> new GasOperationV2(gasCalculator);
      case TLoadOperation ignored -> new TLoadOperationV2(gasCalculator);
      case TStoreOperation ignored -> new TStoreOperationV2(gasCalculator);
      case MCopyOperation ignored -> new MCopyOperationV2(gasCalculator);
      case LogOperation log -> new LogOperationV2(log.getOpcode() - 0xA0, gasCalculator);
      case CreateOperation ignored -> new CreateOperationV2(gasCalculator);
      case CallOperation ignored -> new CallOperationV2(gasCalculator);
      case CallCodeOperation ignored -> new CallCodeOperationV2(gasCalculator);
      case ReturnOperation ignored -> new ReturnOperationV2(gasCalculator);
      case DelegateCallOperation ignored -> new DelegateCallOperationV2(gasCalculator);
      case Create2Operation ignored -> new Create2OperationV2(gasCalculator);
      case StaticCallOperation ignored -> new StaticCallOperationV2(gasCalculator);
      case RevertOperation ignored -> new RevertOperationV2(gasCalculator);
      case PayOperation ignored -> new PayOperationV2(gasCalculator);
      // the v1 operation does not expose its settings, so they follow the forks that introduced
      // them
      case SelfDestructOperation ignored ->
          new SelfDestructOperationV2(
              gasCalculator,
              EvmSpecVersion.CANCUN.ordinal() <= evmSpecVersion.ordinal(),
              EvmSpecVersion.AMSTERDAM.ordinal() <= evmSpecVersion.ordinal()
                  ? EIP7708TransferLogEmitter.INSTANCE
                  : TransferLogEmitter.NOOP);
      default ->
          throw new IllegalArgumentException(
              "EVM v2 has no version of operation " + operation.getName());
    };
  }
}
