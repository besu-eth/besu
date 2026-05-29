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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import static org.hyperledger.besu.datatypes.HardforkId.MainnetHardforkId.CANCUN;

import org.hyperledger.besu.consensus.merge.blockcreation.MergeMiningCoordinator;
import org.hyperledger.besu.datatypes.VersionedHash;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters.EnginePayloadParameter;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.Transaction;
import org.hyperledger.besu.ethereum.eth.manager.EthPeers;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.ValidationResult;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.util.List;
import java.util.Optional;

import io.vertx.core.Vertx;

public class EngineNewPayloadV2 extends AbstractEngineNewPayload {
  private final Optional<Long> cancunMilestone;

  public EngineNewPayloadV2(
      final Vertx vertx,
      final ProtocolSchedule protocolSchedule,
      final ProtocolContext protocolContext,
      final MergeMiningCoordinator mergeCoordinator,
      final EthPeers ethPeers,
      final EngineCallListener engineCallListener,
      final MetricsSystem metricsSystem) {
    super(
        vertx,
        protocolSchedule,
        protocolContext,
        mergeCoordinator,
        ethPeers,
        engineCallListener,
        metricsSystem);
    cancunMilestone = protocolSchedule.milestoneFor(CANCUN);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_NEW_PAYLOAD_V2.getMethodName();
  }

  @Override
  protected ValidationResult<RpcErrorType> validateParameters(
      final EnginePayloadParameter payloadParameter,
      final Optional<List<String>> maybeVersionedHashParam,
      final Optional<String> maybeBeaconBlockRootParam,
      final Optional<List<String>> maybeRequestsParam,
      final Optional<List<String>> maybeInclusionListTransactions) {
    if (payloadParameter.getBlobGasUsed() != null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_BLOB_GAS_USED_PARAMS, "Unexpected blob gas used field present");
    }
    if (payloadParameter.getExcessBlobGas() != null) {
      return ValidationResult.invalid(
          RpcErrorType.INVALID_EXCESS_BLOB_GAS_PARAMS, "Unexpected excess blob gas field present");
    }
    return ValidationResult.valid();
  }

  @Override
  protected ValidationResult<RpcErrorType> validateBlobs(
      final List<Transaction> transactions,
      final BlockHeader header,
      final Optional<BlockHeader> maybeParentHeader,
      final Optional<List<VersionedHash>> maybeVersionedHashParam,
      final ProtocolSpec protocolSpec) {
    return ValidationResult.valid();
  }

  @Override
  protected ValidationResult<RpcErrorType> validateForkSupported(final long blockTimestamp) {
    if (cancunMilestone.isPresent() && blockTimestamp >= cancunMilestone.get()) {
      return ValidationResult.invalid(RpcErrorType.UNSUPPORTED_FORK);
    }

    return ValidationResult.valid();
  }
}
