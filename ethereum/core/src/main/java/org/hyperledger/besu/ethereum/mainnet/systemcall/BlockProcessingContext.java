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
package org.hyperledger.besu.ethereum.mainnet.systemcall;

import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList.BlockAccessListBuilder;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.tracing.OperationTracer;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Optional;

public class BlockProcessingContext {

  private final MutableWorldState worldState;
  private final ProcessableBlockHeader blockHeader;
  private final OperationTracer operationTracer;
  private final BlockHashLookup blockHashLookup;
  private final ProtocolSpec protocolSpec;
  private final Optional<BlockAccessListBuilder> blockAccessListBuilder;

  public BlockProcessingContext(
      final ProcessableBlockHeader blockHeader,
      final MutableWorldState worldState,
      final ProtocolSpec protocolSpec,
      final BlockHashLookup blockHashLookup,
      final OperationTracer operationTracer,
      final Optional<BlockAccessListBuilder> blockAccessListBuilder) {
    this.blockHeader = blockHeader;
    this.worldState = worldState;
    this.protocolSpec = protocolSpec;
    this.blockHashLookup = blockHashLookup;
    this.operationTracer = operationTracer;
    this.blockAccessListBuilder = blockAccessListBuilder;
  }

  public MutableWorldState getWorldState() {
    return worldState;
  }

  public ProcessableBlockHeader getBlockHeader() {
    return blockHeader;
  }

  public OperationTracer getOperationTracer() {
    return operationTracer;
  }

  public BlockHashLookup getBlockHashLookup() {
    return blockHashLookup;
  }

  public ProtocolSpec getProtocolSpec() {
    return protocolSpec;
  }

  public Optional<BlockAccessListBuilder> getBlockAccessListBuilder() {
    return blockAccessListBuilder;
  }
}
