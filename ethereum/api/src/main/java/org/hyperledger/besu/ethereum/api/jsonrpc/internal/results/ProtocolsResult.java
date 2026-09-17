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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.results;

import org.hyperledger.besu.ethereum.eth.manager.ChainState.BestBlock;
import org.hyperledger.besu.ethereum.eth.manager.EthPeer;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import org.apache.tuweni.bytes.Bytes;
import org.immutables.value.Value;

@JsonPropertyOrder({"difficulty", "head", "latestBlock", "version"})
@Value.Immutable
@Value.Style(allParameters = true)
public interface ProtocolsResult {

  static ProtocolsResult fromEthPeer(final EthPeer ethPeer) {
    final BestBlock bestBlock = ethPeer.chainState().getBestBlock();
    return ImmutableProtocolsResult.builder()
        .difficulty(Quantity.create(bestBlock.getTotalDifficulty()))
        .head(
            bestBlock.getHash() != null
                ? bestBlock.getHash().getBytes().toHexString()
                : Bytes.EMPTY.toHexString())
        .latestBlock(bestBlock.getNumber())
        .version(ethPeer.getLastProtocolVersion())
        .build();
  }

  String getDifficulty();

  String getHead();

  long getLatestBlock();

  int getVersion();
}
