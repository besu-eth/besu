/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.api.jsonrpc.methods;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.api.ApiConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcApis;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter.FilterManager;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthAccounts;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthBlobBaseFee;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthBlockNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthCall;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthChainId;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthConfig;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthCreateAccessList;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthEstimateGas;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthFeeHistory;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGasPrice;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBalance;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockAccessList;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockByHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockByNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockReceipts;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockTransactionCountByHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetBlockTransactionCountByNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetCode;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetFilterChanges;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetFilterLogs;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetLogs;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetProof;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetStorageAt;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetTransactionByBlockHashAndIndex;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetTransactionByBlockNumberAndIndex;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetTransactionByHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetTransactionCount;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetTransactionReceipt;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetUncleByBlockHashAndIndex;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetUncleByBlockNumberAndIndex;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetUncleCountByBlockHash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthGetUncleCountByBlockNumber;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthMaxPriorityFeePerGas;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthMining;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthNewBlockFilter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthNewFilter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthNewPendingTransactionFilter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthProtocolVersion;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthSendRawTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthSendTransaction;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthSimulateV1;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthSyncing;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.EthUninstallFilter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.BlockResultFactory;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.ethereum.core.Synchronizer;
import org.hyperledger.besu.ethereum.eth.transactions.TransactionPool;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.p2p.rlpx.wire.Capability;
import org.hyperledger.besu.ethereum.transaction.TransactionSimulator;
import org.hyperledger.besu.plugin.ServiceManager;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.Map;
import java.util.Set;

public class EthJsonRpcMethods extends ApiGroupJsonRpcMethods {

  private final BlockResultFactory blockResult = new BlockResultFactory();

  private final BlockchainQueries blockchainQueries;
  private final Synchronizer synchronizer;
  private final ProtocolSchedule protocolSchedule;
  private final FilterManager filterManager;
  private final TransactionPool transactionPool;
  private final MiningCoordinator miningCoordinator;
  private final MiningConfiguration miningConfiguration;

  private final Set<Capability> supportedCapabilities;
  private final ApiConfiguration apiConfiguration;
  private final GenesisConfigOptions genesisConfigOptions;
  private final TransactionSimulator transactionSimulator;
  private final ServiceManager serviceManager;
  private final MetricsSystem metricsSystem;

  public EthJsonRpcMethods(
      final BlockchainQueries blockchainQueries,
      final Synchronizer synchronizer,
      final ProtocolSchedule protocolSchedule,
      final FilterManager filterManager,
      final TransactionPool transactionPool,
      final MiningCoordinator miningCoordinator,
      final MiningConfiguration miningConfiguration,
      final Set<Capability> supportedCapabilities,
      final ApiConfiguration apiConfiguration,
      final GenesisConfigOptions genesisConfigOptions,
      final TransactionSimulator transactionSimulator,
      final ServiceManager serviceManager,
      final MetricsSystem metricsSystem) {
    this.blockchainQueries = blockchainQueries;
    this.synchronizer = synchronizer;
    this.protocolSchedule = protocolSchedule;
    this.filterManager = filterManager;
    this.transactionPool = transactionPool;
    this.miningCoordinator = miningCoordinator;
    this.miningConfiguration = miningConfiguration;
    this.supportedCapabilities = supportedCapabilities;
    this.apiConfiguration = apiConfiguration;
    this.genesisConfigOptions = genesisConfigOptions;
    this.transactionSimulator = transactionSimulator;
    this.serviceManager = serviceManager;
    this.metricsSystem = metricsSystem;
  }

  @Override
  protected String getApiGroup() {
    return RpcApis.ETH.name();
  }

  @Override
  protected Map<String, JsonRpcMethod> create() {
    final Map<String, JsonRpcMethod> map =
        mapOf(
            new EthAccounts(),
            new EthBlockNumber(blockchainQueries),
            new EthGetBalance(blockchainQueries),
            new EthGetBlockByHash(blockchainQueries, blockResult),
            new EthGetBlockByNumber(blockchainQueries, blockResult, synchronizer),
            new EthGetBlockReceipts(blockchainQueries, protocolSchedule),
            new EthGetBlockTransactionCountByNumber(blockchainQueries),
            new EthGetBlockTransactionCountByHash(blockchainQueries),
            new EthCall(blockchainQueries, transactionSimulator, metricsSystem),
            new EthFeeHistory(
                protocolSchedule, blockchainQueries, miningCoordinator, apiConfiguration),
            new EthGetCode(blockchainQueries),
            new EthGetLogs(blockchainQueries, apiConfiguration.getMaxLogsRange()),
            new EthGetProof(blockchainQueries),
            new EthGetUncleCountByBlockHash(blockchainQueries),
            new EthGetUncleCountByBlockNumber(blockchainQueries),
            new EthGetUncleByBlockNumberAndIndex(blockchainQueries),
            new EthGetUncleByBlockHashAndIndex(blockchainQueries),
            new EthNewBlockFilter(filterManager),
            new EthNewPendingTransactionFilter(filterManager),
            new EthNewFilter(filterManager),
            new EthGetTransactionByHash(blockchainQueries, transactionPool),
            new EthGetTransactionByBlockHashAndIndex(blockchainQueries),
            new EthGetTransactionByBlockNumberAndIndex(blockchainQueries),
            new EthGetTransactionCount(blockchainQueries, transactionPool),
            new EthGetTransactionReceipt(blockchainQueries, protocolSchedule),
            new EthUninstallFilter(filterManager),
            new EthGetFilterChanges(filterManager),
            new EthGetFilterLogs(filterManager),
            new EthSyncing(synchronizer),
            new EthGetStorageAt(blockchainQueries),
            new EthSendRawTransaction(transactionPool),
            new EthSendTransaction(),
            new EthEstimateGas(blockchainQueries, transactionSimulator, apiConfiguration),
            new EthCreateAccessList(blockchainQueries, transactionSimulator),
            new EthMining(miningCoordinator),
            new EthConfig(blockchainQueries, protocolSchedule, genesisConfigOptions),
            new EthProtocolVersion(supportedCapabilities),
            new EthGasPrice(blockchainQueries, apiConfiguration),
            new EthChainId(protocolSchedule.getChainId()),
            new EthBlobBaseFee(blockchainQueries.getBlockchain(), protocolSchedule),
            new EthMaxPriorityFeePerGas(blockchainQueries),
            new EthSimulateV1(
                serviceManager,
                blockchainQueries,
                protocolSchedule,
                transactionSimulator,
                miningConfiguration,
                apiConfiguration),
            new EthGetBlockAccessList(blockchainQueries));
    return map;
  }
}
