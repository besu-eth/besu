/*
 * Copyright 2019 ConsenSys AG.
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
package org.hyperledger.besu.consensus.common;

import org.hyperledger.besu.cryptoservices.NodeKey;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.plugin.data.BlockHeader;
import org.hyperledger.besu.plugin.services.query.PoaQueryService;

import java.util.ArrayList;
import java.util.Collection;

/** The Poa query service. */
public class PoaQueryServiceImpl implements PoaQueryService {

  private final BlockInterface blockInterface;
  private final Blockchain blockchain;
  private final NodeKey nodeKey;

  /**
   * Instantiates a new Poa query service.
   *
   * @param blockInterface the block interface
   * @param blockchain the blockchain
   * @param nodeKey the node key
   */
  public PoaQueryServiceImpl(
      final BlockInterface blockInterface, final Blockchain blockchain, final NodeKey nodeKey) {
    this.blockInterface = blockInterface;
    this.blockchain = blockchain;
    this.nodeKey = nodeKey;
  }

  @Override
  public Collection<Address> getValidatorsForLatestBlock() {
    return new ArrayList<>(blockInterface.validatorsInBlock(blockchain.getChainHeadHeader()));
  }

  @Override
  public Address getProposerOfBlock(final BlockHeader header) {
    return this.blockInterface.getProposerOfBlock(header);
  }

  /**
   * Gets blockchain.
   *
   * @return the blockchain
   */
  protected Blockchain getBlockchain() {
    return blockchain;
  }

  @Override
  public Address getLocalSignerAddress() {
    return org.hyperledger.besu.datatypes.Address.extract(nodeKey.getPublicKey());
  }
}
