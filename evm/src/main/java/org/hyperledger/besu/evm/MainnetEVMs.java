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
package org.hyperledger.besu.evm;

import org.hyperledger.besu.evm.gascalculator.BerlinGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ByzantiumGasCalculator;
import org.hyperledger.besu.evm.gascalculator.CancunGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ConstantinopleGasCalculator;
import org.hyperledger.besu.evm.gascalculator.FrontierGasCalculator;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;
import org.hyperledger.besu.evm.gascalculator.HomesteadGasCalculator;
import org.hyperledger.besu.evm.gascalculator.IstanbulGasCalculator;
import org.hyperledger.besu.evm.gascalculator.LondonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.OsakaGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PetersburgGasCalculator;
import org.hyperledger.besu.evm.gascalculator.PragueGasCalculator;
import org.hyperledger.besu.evm.gascalculator.ShanghaiGasCalculator;
import org.hyperledger.besu.evm.gascalculator.SpuriousDragonGasCalculator;
import org.hyperledger.besu.evm.gascalculator.TangerineWhistleGasCalculator;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.log.EIP7708TransferLogEmitter;
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
import org.hyperledger.besu.evm.operation.NotOperation;
import org.hyperledger.besu.evm.operation.NotOperationOptimized;
import org.hyperledger.besu.evm.operation.NumberOperation;
import org.hyperledger.besu.evm.operation.OperationRegistry;
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
import org.hyperledger.besu.evm.operation.SwapNOperation;
import org.hyperledger.besu.evm.operation.SwapOperation;
import org.hyperledger.besu.evm.operation.TLoadOperation;
import org.hyperledger.besu.evm.operation.TStoreOperation;
import org.hyperledger.besu.evm.operation.TimestampOperation;
import org.hyperledger.besu.evm.operation.XorOperation;
import org.hyperledger.besu.evm.operation.XorOperationOptimized;

import java.math.BigInteger;

import org.apache.tuweni.bytes.Bytes;
import org.apache.tuweni.bytes.Bytes32;

/** Provides EVMs supporting the appropriate operations for mainnet hard forks. */
public class MainnetEVMs {

  /** The constant DEV_NET_CHAIN_ID. */
  public static final BigInteger DEV_NET_CHAIN_ID = BigInteger.valueOf(1337);

  private MainnetEVMs() {
    // utility class
  }

  /**
   * Frontier evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM frontier(final EvmConfiguration evmConfiguration) {
    return frontier(new FrontierGasCalculator(), evmConfiguration);
  }

  /**
   * Frontier evm.
   *
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM frontier(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    return new EVM(
        frontierOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.FRONTIER);
  }

  /**
   * Operation registry for frontier's operations.
   *
   * @param gasCalculator the gas calculator
   * @return the operation registry
   */
  private static OperationRegistry frontierOperations(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerFrontierOperations(operationRegistry, gasCalculator, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register frontier operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  private static void registerFrontierOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration) {
    for (int i = 0; i < 255; i++) {
      registry.put(new InvalidOperation(i, gasCalculator));
    }
    registry.put(new MulOperation(gasCalculator));
    registry.put(new SubOperation(gasCalculator));
    if (evmConfiguration.enableOptimizedOpcodes()) {
      registry.put(new AddOperationOptimized(gasCalculator));
      registry.put(new ModOperationOptimized(gasCalculator));
      registry.put(new SModOperationOptimized(gasCalculator));
      registry.put(new AddModOperationOptimized(gasCalculator));
      registry.put(new MulModOperationOptimized(gasCalculator));
      registry.put(new AndOperationOptimized(gasCalculator));
      registry.put(new XorOperationOptimized(gasCalculator));
      registry.put(new OrOperationOptimized(gasCalculator));
      registry.put(new NotOperationOptimized(gasCalculator));
      registry.put(new DivOperationOptimized(gasCalculator));
      registry.put(new SDivOperationOptimized(gasCalculator));
    } else {
      registry.put(new AddOperation(gasCalculator));
      registry.put(new ModOperation(gasCalculator));
      registry.put(new SModOperation(gasCalculator));
      registry.put(new AddModOperation(gasCalculator));
      registry.put(new MulModOperation(gasCalculator));
      registry.put(new AndOperation(gasCalculator));
      registry.put(new XorOperation(gasCalculator));
      registry.put(new OrOperation(gasCalculator));
      registry.put(new NotOperation(gasCalculator));
      registry.put(new DivOperation(gasCalculator));
      registry.put(new SDivOperation(gasCalculator));
    }
    registry.put(new ExpOperation(gasCalculator));
    registry.put(new SignExtendOperation(gasCalculator));
    registry.put(new LtOperation(gasCalculator));
    registry.put(new GtOperation(gasCalculator));
    registry.put(new SLtOperation(gasCalculator));
    registry.put(new SGtOperation(gasCalculator));
    registry.put(new EqOperation(gasCalculator));
    registry.put(new IsZeroOperation(gasCalculator));
    registry.put(new ByteOperation(gasCalculator));
    registry.put(new Keccak256Operation(gasCalculator));
    registry.put(new AddressOperation(gasCalculator));
    registry.put(new BalanceOperation(gasCalculator));
    registry.put(new OriginOperation(gasCalculator));
    registry.put(new CallerOperation(gasCalculator));
    registry.put(new CallValueOperation(gasCalculator));
    registry.put(new CallDataLoadOperation(gasCalculator));
    registry.put(new CallDataSizeOperation(gasCalculator));
    registry.put(new CallDataCopyOperation(gasCalculator));
    registry.put(new CodeSizeOperation(gasCalculator));
    registry.put(new CodeCopyOperation(gasCalculator));
    registry.put(new GasPriceOperation(gasCalculator));
    registry.put(new ExtCodeCopyOperation(gasCalculator));
    registry.put(new ExtCodeSizeOperation(gasCalculator));
    registry.put(new BlockHashOperation(gasCalculator));
    registry.put(new CoinbaseOperation(gasCalculator));
    registry.put(new TimestampOperation(gasCalculator));
    registry.put(new NumberOperation(gasCalculator));
    registry.put(new DifficultyOperation(gasCalculator));
    registry.put(new GasLimitOperation(gasCalculator));
    registry.put(new PopOperation(gasCalculator));
    registry.put(new MLoadOperation(gasCalculator));
    registry.put(new MStoreOperation(gasCalculator));
    registry.put(new MStore8Operation(gasCalculator));
    registry.put(new SLoadOperation(gasCalculator));
    registry.put(new SStoreOperation(gasCalculator, SStoreOperation.FRONTIER_MINIMUM));
    registry.put(new JumpOperation(gasCalculator));
    registry.put(new JumpiOperation(gasCalculator));
    registry.put(new PCOperation(gasCalculator));
    registry.put(new MSizeOperation(gasCalculator));
    registry.put(new GasOperation(gasCalculator));
    registry.put(new JumpDestOperation(gasCalculator));
    registry.put(new ReturnOperation(gasCalculator));
    registry.put(new InvalidOperation(gasCalculator));
    registry.put(new StopOperation(gasCalculator));
    registry.put(new SelfDestructOperation(gasCalculator));
    registry.put(new CreateOperation(gasCalculator));
    registry.put(new CallOperation(gasCalculator));
    registry.put(new CallCodeOperation(gasCalculator));

    // Register the PUSH1, PUSH2, ..., PUSH32 operations.
    for (int i = 1; i <= 32; ++i) {
      registry.put(new PushOperation(i, gasCalculator));
    }

    // Register the DUP1, DUP2, ..., DUP16 operations.
    for (int i = 1; i <= 16; ++i) {
      registry.put(new DupOperation(i, gasCalculator));
    }

    // Register the SWAP1, SWAP2, ..., SWAP16 operations.
    for (int i = 1; i <= 16; ++i) {
      registry.put(new SwapOperation(i, gasCalculator));
    }

    // Register the LOG0, LOG1, ..., LOG4 operations.
    for (int i = 0; i < 5; ++i) {
      registry.put(new LogOperation(i, gasCalculator));
    }
  }

  /**
   * Homestead evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM homestead(final EvmConfiguration evmConfiguration) {
    return homestead(new HomesteadGasCalculator(), evmConfiguration);
  }

  /**
   * Homestead evm.
   *
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM homestead(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    return new EVM(
        homesteadOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.HOMESTEAD);
  }

  /**
   * Operation registry for homestead's operations.
   *
   * @param gasCalculator the gas calculator
   * @return the operation registry
   */
  private static OperationRegistry homesteadOperations(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerHomesteadOperations(operationRegistry, gasCalculator, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register homestead operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  private static void registerHomesteadOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration) {
    registerFrontierOperations(registry, gasCalculator, evmConfiguration);
    registry.put(new DelegateCallOperation(gasCalculator));
  }

  /**
   * Spurious dragon evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM spuriousDragon(final EvmConfiguration evmConfiguration) {
    GasCalculator gasCalculator = new SpuriousDragonGasCalculator();
    return new EVM(
        homesteadOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.SPURIOUS_DRAGON);
  }

  /**
   * Tangerine whistle evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM tangerineWhistle(final EvmConfiguration evmConfiguration) {
    GasCalculator gasCalculator = new TangerineWhistleGasCalculator();
    return new EVM(
        homesteadOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.TANGERINE_WHISTLE);
  }

  /**
   * Byzantium evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM byzantium(final EvmConfiguration evmConfiguration) {
    return byzantium(new ByzantiumGasCalculator(), evmConfiguration);
  }

  /**
   * Byzantium evm.
   *
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM byzantium(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    return new EVM(
        byzantiumOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.BYZANTIUM);
  }

  /**
   * Operation registry for byzantium's operations.
   *
   * @param gasCalculator the gas calculator
   * @return the operation registry
   */
  private static OperationRegistry byzantiumOperations(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerByzantiumOperations(operationRegistry, gasCalculator, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register byzantium operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  private static void registerByzantiumOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration) {
    registerHomesteadOperations(registry, gasCalculator, evmConfiguration);
    registry.put(new ReturnDataCopyOperation(gasCalculator));
    registry.put(new ReturnDataSizeOperation(gasCalculator));
    registry.put(new RevertOperation(gasCalculator));
    registry.put(new StaticCallOperation(gasCalculator));
  }

  /**
   * Constantinople evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM constantinople(final EvmConfiguration evmConfiguration) {
    return constantinople(new ConstantinopleGasCalculator(), evmConfiguration);
  }

  /**
   * Constantinople evm.
   *
   * @param gasCalculator the gas calculator
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM constantinople(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    var version = EvmSpecVersion.CONSTANTINOPLE;
    return constantiNOPEl(gasCalculator, evmConfiguration, version);
  }

  private static EVM constantiNOPEl(
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration,
      final EvmSpecVersion version) {
    return new EVM(
        constantinopleOperations(gasCalculator, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        version);
  }

  /**
   * Operation registry for constantinople's operations.
   *
   * @param gasCalculator the gas calculator
   * @return the operation registry
   */
  private static OperationRegistry constantinopleOperations(
      final GasCalculator gasCalculator, final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerConstantinopleOperations(operationRegistry, gasCalculator, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register constantinople operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  private static void registerConstantinopleOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final EvmConfiguration evmConfiguration) {
    registerByzantiumOperations(registry, gasCalculator, evmConfiguration);
    registry.put(new Create2Operation(gasCalculator));
    if (evmConfiguration.enableOptimizedOpcodes()) {
      registry.put(new ShlOperationOptimized(gasCalculator));
      registry.put(new ShrOperationOptimized(gasCalculator));
      registry.put(new SarOperationOptimized(gasCalculator));
    } else {
      registry.put(new ShlOperation(gasCalculator));
      registry.put(new ShrOperation(gasCalculator));
      registry.put(new SarOperation(gasCalculator));
    }
    registry.put(new ExtCodeHashOperation(gasCalculator));
  }

  /**
   * Petersburg evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM petersburg(final EvmConfiguration evmConfiguration) {
    return constantiNOPEl(
        new PetersburgGasCalculator(), evmConfiguration, EvmSpecVersion.PETERSBURG);
  }

  /**
   * Istanbul evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM istanbul(final EvmConfiguration evmConfiguration) {
    return istanbul(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Istanbul evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM istanbul(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return istanbul(new IstanbulGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Istanbul evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM istanbul(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        istanbulOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.ISTANBUL);
  }

  /**
   * Operation registry for istanbul's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry istanbulOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerIstanbulOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register istanbul operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   */
  static void registerIstanbulOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    registerConstantinopleOperations(registry, gasCalculator, evmConfiguration);
    registry.put(
        new ChainIdOperation(gasCalculator, Bytes32.leftPad(Bytes.of(chainId.toByteArray()))));
    registry.put(new SelfBalanceOperation(gasCalculator));
    registry.put(new SStoreOperation(gasCalculator, SStoreOperation.EIP_1706_MINIMUM));
  }

  /**
   * Berlin evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM berlin(final EvmConfiguration evmConfiguration) {
    return berlin(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Berlin evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM berlin(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return berlin(new BerlinGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Berlin evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM berlin(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        istanbulOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.BERLIN);
  }

  /**
   * London evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM london(final EvmConfiguration evmConfiguration) {
    return london(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * London evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM london(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return london(new LondonGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * London evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM london(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        londonOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.LONDON);
  }

  /**
   * Operation registry for london's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry londonOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerLondonOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register london operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   */
  private static void registerLondonOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    registerIstanbulOperations(registry, gasCalculator, chainId, evmConfiguration);
    registry.put(new BaseFeeOperation(gasCalculator));
  }

  /**
   * Paris evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM paris(final EvmConfiguration evmConfiguration) {
    return paris(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Paris evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM paris(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return paris(new LondonGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Paris evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM paris(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        parisOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.PARIS);
  }

  /**
   * Operation registry for paris's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry parisOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerParisOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register paris operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerParisOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerLondonOperations(registry, gasCalculator, chainID, evmConfiguration);
    registry.put(new PrevRanDaoOperation(gasCalculator));
  }

  /**
   * Shanghai evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM shanghai(final EvmConfiguration evmConfiguration) {
    return shanghai(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Shanghai evm
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM shanghai(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return shanghai(new ShanghaiGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * shanghai evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM shanghai(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        shanghaiOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.SHANGHAI);
  }

  /**
   * shanghai operations registry.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry shanghaiOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerShanghaiOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register Shanghai operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerShanghaiOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerParisOperations(registry, gasCalculator, chainID, evmConfiguration);
    registry.put(new Push0Operation(gasCalculator));
  }

  /**
   * Cancun evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM cancun(final EvmConfiguration evmConfiguration) {
    return cancun(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Cancun evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM cancun(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return cancun(new CancunGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Cancun evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM cancun(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        cancunOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.CANCUN);
  }

  /**
   * Operation registry for cancun's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry cancunOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerCancunOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register cancun operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerCancunOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerShanghaiOperations(registry, gasCalculator, chainID, evmConfiguration);

    // EIP-1153 TSTORE/TLOAD
    registry.put(new TStoreOperation(gasCalculator));
    registry.put(new TLoadOperation(gasCalculator));

    // EIP-4844 BLOBHASH
    registry.put(new BlobHashOperation(gasCalculator));

    // EIP-5656 MCOPY
    registry.put(new MCopyOperation(gasCalculator));

    // EIP-6780 nerf self destruct
    registry.put(new SelfDestructOperation(gasCalculator, true));

    // EIP-7516 BLOBBASEFEE
    registry.put(new BlobBaseFeeOperation(gasCalculator));
  }

  /**
   * Prague evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM prague(final EvmConfiguration evmConfiguration) {
    return prague(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Prague evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM prague(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return prague(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Prague evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM prague(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        pragueOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.PRAGUE);
  }

  /**
   * Operation registry for prague's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry pragueOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerPragueOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register prague operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerPragueOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerCancunOperations(registry, gasCalculator, chainID, evmConfiguration);
  }

  /**
   * Osaka evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM osaka(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return osaka(new OsakaGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Osaka evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM osaka(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        osakaOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.OSAKA);
  }

  /**
   * Operation registry for Osaka's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry osakaOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerOsakaOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register Osaka's operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerOsakaOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerPragueOperations(registry, gasCalculator, chainID, evmConfiguration);

    // EIP-7939: CLZ opcode
    registry.put(new CountLeadingZerosOperation(gasCalculator));
  }

  /**
   * Amsterdam evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM amsterdam(final EvmConfiguration evmConfiguration) {
    return amsterdam(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Amsterdam evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM amsterdam(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return amsterdam(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Amsterdam evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM amsterdam(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        amsterdamOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.AMSTERDAM);
  }

  /**
   * Operation registry for amsterdam's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry amsterdamOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerAmsterdamOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register amsterdam operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerAmsterdamOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerOsakaOperations(registry, gasCalculator, chainID, evmConfiguration);

    // EIP-7708: SelfDestruct with transfer log emission
    registry.put(
        new SelfDestructOperation(gasCalculator, true, EIP7708TransferLogEmitter.INSTANCE));

    // EIP-7843 SLOTNUM opcode
    registry.put(new SlotNumOperation(gasCalculator));

    // EIP-8024: DUPN, SWAPN, EXCHANGE
    registry.put(new DupNOperation(gasCalculator));
    registry.put(new SwapNOperation(gasCalculator));
    registry.put(new ExchangeOperation(gasCalculator));
  }

  /**
   * Bogota evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bogota(final EvmConfiguration evmConfiguration) {
    return bogota(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Bogota evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bogota(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return bogota(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Bogota evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bogota(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        bogotaOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.BOGOTA);
  }

  /**
   * Bogota operation registry.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry bogotaOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerBogotaOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register bogota operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerBogotaOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerAmsterdamOperations(registry, gasCalculator, chainID, evmConfiguration);
  }

  /**
   * Polis evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM polis(final EvmConfiguration evmConfiguration) {
    return polis(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Polis evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM polis(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return polis(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Polis evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM polis(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        polisOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.POLIS);
  }

  /**
   * Operation registry for Polis's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry polisOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerPolisOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register polis operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerPolisOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerBogotaOperations(registry, gasCalculator, chainID, evmConfiguration);
  }

  /**
   * Bangkok evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bangkok(final EvmConfiguration evmConfiguration) {
    return bangkok(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Bangkok evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bangkok(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return bangkok(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Bangkok evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM bangkok(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        bangkokOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.BANGKOK);
  }

  /**
   * Operation registry for bangkok's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry bangkokOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerBangkokOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register bangkok operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerBangkokOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerPolisOperations(registry, gasCalculator, chainID, evmConfiguration);
  }

  /**
   * Future eips evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM futureEips(final EvmConfiguration evmConfiguration) {
    return futureEips(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Future eips evm.
   *
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM futureEips(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return futureEips(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Future eips evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM futureEips(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        futureEipsOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.FUTURE_EIPS);
  }

  /**
   * Future Operation registry for eIPs's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry futureEipsOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerFutureEipsOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register FutureEIPs operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerFutureEipsOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerBogotaOperations(registry, gasCalculator, chainID, evmConfiguration);

    // EIP-5920 PAY opcode
    registry.put(new PayOperation(gasCalculator));
  }

  /**
   * Experimental eips evm.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM experimentalEips(final EvmConfiguration evmConfiguration) {
    return experimentalEips(DEV_NET_CHAIN_ID, evmConfiguration);
  }

  /**
   * Experimental eips evm.
   *
   * @param chainId the chain Id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM experimentalEips(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    return experimentalEips(new PragueGasCalculator(), chainId, evmConfiguration);
  }

  /**
   * Experimental eips evm.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @param evmConfiguration the evm configuration
   * @return the evm
   */
  public static EVM experimentalEips(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return new EVM(
        experimentalEipsOperations(gasCalculator, chainId, evmConfiguration),
        gasCalculator,
        evmConfiguration,
        EvmSpecVersion.EXPERIMENTAL_EIPS);
  }

  /**
   * Operation registry for experimental's operations.
   *
   * @param gasCalculator the gas calculator
   * @param chainId the chain id
   * @return the operation registry
   */
  private static OperationRegistry experimentalEipsOperations(
      final GasCalculator gasCalculator,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    OperationRegistry operationRegistry = new OperationRegistry();
    registerExperimentalEipsOperations(operationRegistry, gasCalculator, chainId, evmConfiguration);
    return operationRegistry;
  }

  /**
   * Register experimental eips operations.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   * @param chainID the chain id
   */
  private static void registerExperimentalEipsOperations(
      final OperationRegistry registry,
      final GasCalculator gasCalculator,
      final BigInteger chainID,
      final EvmConfiguration evmConfiguration) {
    registerFutureEipsOperations(registry, gasCalculator, chainID, evmConfiguration);
  }
}
