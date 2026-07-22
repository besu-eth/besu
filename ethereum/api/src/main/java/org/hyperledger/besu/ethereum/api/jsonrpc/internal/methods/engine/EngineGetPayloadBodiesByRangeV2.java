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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.datatypes.HardforkId;
import org.hyperledger.besu.ethereum.api.jsonrpc.RpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.results.ExecutionPayloadBodiesV2;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockBody;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;

public final class EngineGetPayloadBodiesByRangeV2<EPB extends ExecutionPayloadBodiesV2>
    extends EngineGetPayloadBodiesByRangeV1<EPB> {

  public EngineGetPayloadBodiesByRangeV2(
      final ConstructorArguments constructorArguments,
      final HardforkId minSupportedFork,
      final HardforkId firstUnsupportedFork) {
    super(constructorArguments, minSupportedFork, firstUnsupportedFork);
  }

  @Override
  public String getName() {
    return RpcMethod.ENGINE_GET_PAYLOAD_BODIES_BY_RANGE_V2.getMethodName();
  }

  @Override
  @SuppressWarnings("unchecked")
  protected EPB fetchExecutionPayloadBody(final Block block) {
    final BlockBody body = block.getBody();
    final BlockAccessList blockAccessList =
        blockchain.getBlockAccessList(block.getHash()).orElse(null);
    return (EPB)
        new ExecutionPayloadBodiesV2(
            body.getTransactions(), body.getWithdrawals().orElse(null), blockAccessList);
  }
}
