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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.INVALID;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.SYNCING;
import static org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus.VALID;

import org.hyperledger.besu.consensus.merge.blockcreation.PayloadIdentifier;
import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.ExecutionEngineJsonRpcMethod.EngineStatus;

import java.util.EnumSet;
import java.util.Optional;

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;

@JsonPropertyOrder({"payloadStatus", "payloadId"})
public class EngineUpdateForkchoiceResult {
  private final EnginePayloadStatusResult payloadStatus;
  private final PayloadIdentifier payloadId;
  static final EnumSet<EngineStatus> FORK_CHOICE_ENGINE_STATUS =
      EnumSet.of(VALID, INVALID, SYNCING);

  public EngineUpdateForkchoiceResult(final EngineStatus status, final Hash latestValidHash) {
    this(status, latestValidHash, null);
  }

  public EngineUpdateForkchoiceResult(
      final EngineStatus status, final Hash latestValidHash, final PayloadIdentifier payloadId) {
    this(status, latestValidHash, payloadId, Optional.empty());
  }

  public EngineUpdateForkchoiceResult(
      final EngineStatus status,
      final Hash latestValidHash,
      final PayloadIdentifier payloadId,
      final Optional<String> errorMessage) {

    if (!FORK_CHOICE_ENGINE_STATUS.contains(status)) {
      throw new IllegalStateException(
          String.format("Invalid status response %s for EngineForkChoiceResult", status.name()));
    }

    this.payloadStatus = new EnginePayloadStatusResult(status, latestValidHash, errorMessage);
    this.payloadId = payloadId;
  }

  @JsonGetter(value = "payloadStatus")
  public EnginePayloadStatusResult getPayloadStatus() {
    return payloadStatus;
  }

  @JsonGetter(value = "payloadId")
  public String getPayloadId() {
    return Optional.ofNullable(payloadId).map(PayloadIdentifier::toHexString).orElse(null);
  }
}
