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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods;

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.JsonRpcRequestContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcSuccessResponse;
import org.hyperledger.besu.ethereum.api.query.BlockchainQueries;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.forkid.ForkIdManager;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ScheduledProtocolSpec.Hardfork;
import org.hyperledger.besu.ethereum.mainnet.requests.RequestProcessorCoordinator;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;

import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.TreeMap;
import java.util.function.Supplier;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.common.base.Suppliers;

public class EthConfig implements JsonRpcMethod {

  private static final Supplier<ObjectMapper> mapperSupplier = Suppliers.memoize(ObjectMapper::new);

  private final BlockchainQueries blockchain;
  private final ProtocolSchedule protocolSchedule;
  private final ForkIdManager forkIdManager;

  public EthConfig(
      final BlockchainQueries blockchain,
      final ProtocolSchedule protocolSchedule,
      final GenesisConfigOptions genesisConfigOptions) {

    this.blockchain = blockchain;
    this.protocolSchedule = protocolSchedule;
    forkIdManager =
        new ForkIdManager(
            blockchain.getBlockchain(),
            genesisConfigOptions.getForkIdBlockNumbers(),
            genesisConfigOptions.getForkIdBlockTimestamps());
  }

  @Override
  public String getName() {
    return RpcMethod.ETH_CONFIG.getMethodName();
  }

  @Override
  public JsonRpcResponse response(final JsonRpcRequestContext requestContext) {
    BlockHeader header = blockchain.getBlockchain().getChainHeadHeader();
    long currentTime = System.currentTimeMillis() / 1000;
    ProtocolSpec current = protocolSchedule.getForNextBlockHeader(header, currentTime);
    Optional<ScheduledProtocolSpec> next = protocolSchedule.getNextProtocolSpec(currentTime);
    Optional<ScheduledProtocolSpec> last = protocolSchedule.getLatestProtocolSpec();

    ObjectNode result = mapperSupplier.get().createObjectNode();
    ObjectNode currentNode = result.putObject("current");
    generateConfig(currentNode, current);
    if (next.isPresent()) {
      // if next is present, last will be present as next may be last.
      ObjectNode nextNode = result.putObject("next");
      ScheduledProtocolSpec nextSpec = next.get();
      generateConfig(nextNode, nextSpec);
      ObjectNode lastNode = result.putObject("last");
      generateConfig(lastNode, last.orElse(nextSpec));
    } else {
      // if next is empty, last is empty (no future forks)
      result.putNull("next");
      result.putNull("last");
    }

    return new JsonRpcSuccessResponse(requestContext.getRequest().getId(), result);
  }

  private String getForkIdAsHexString(final long currentTime) {
    return forkIdManager.getForkIdByTimestamp(currentTime).getHash().toHexString();
  }

  void generateConfig(final ObjectNode result, final ScheduledProtocolSpec scheduledSpec) {
    generateConfig(result, scheduledSpec.fork(), scheduledSpec.spec());
  }

  void generateConfig(final ObjectNode result, final ProtocolSpec spec) {
    generateConfig(result, protocolSchedule.hardforkFor(x -> x.spec() == spec).orElseThrow(), spec);
  }

  void generateConfig(final ObjectNode result, final Hardfork forkId, final ProtocolSpec spec) {
    result.put("activationTime", forkId.milestone());

    ObjectNode blobs = result.putObject("blobSchedule");
    blobs.put(
        "baseFeeUpdateFraction", spec.getFeeMarket().getBaseFeeUpdateFraction().longValueExact());
    blobs.put("max", spec.getGasLimitCalculator().currentBlobGasLimit() / (128 * 1024));
    blobs.put("target", spec.getGasLimitCalculator().getTargetBlobGasPerBlock() / (128 * 1024));

    result.put(
        "chainId", protocolSchedule.getChainId().map(c -> "0x" + c.toString(16)).orElse(null));

    result.put("forkId", getForkIdAsHexString(forkId.milestone()));

    PrecompileContractRegistry registry = spec.getPrecompileContractRegistry();
    ObjectNode precompiles = result.putObject("precompiles");
    registry.getPrecompileAddresses().stream()
        .map(a -> Map.entry(registry.get(a).getName(), a.getBytes().toHexString()))
        .sorted(Entry.comparingByKey())
        .forEach(e -> precompiles.put(e.getKey(), e.getValue()));

    TreeMap<String, String> systemContracts =
        new TreeMap<>(
            spec.getRequestProcessorCoordinator()
                .map(RequestProcessorCoordinator::getContractConfigs)
                .orElse(Map.of()));
    spec.getPreExecutionProcessor()
        .getHistoryContract()
        .ifPresent(a -> systemContracts.put("HISTORY_STORAGE_ADDRESS", a.getBytes().toHexString()));
    spec.getPreExecutionProcessor()
        .getBeaconRootsContract()
        .ifPresent(a -> systemContracts.put("BEACON_ROOTS_ADDRESS", a.getBytes().toHexString()));
    if (!systemContracts.isEmpty()) {
      ObjectNode jsonContracts = result.putObject("systemContracts");
      systemContracts.forEach(jsonContracts::put);
    }
  }
}
