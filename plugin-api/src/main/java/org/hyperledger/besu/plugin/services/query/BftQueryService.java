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
package org.hyperledger.besu.plugin.services.query;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Collection;

/** Allows for the BFT specific aspects of the block chain to be queried. */
public interface BftQueryService extends PoaQueryService {

  /**
   * Extracts the round number from the supplied header and returns it to the caller.
   *
   * @param header the block header from which the round number is to be extracted
   * @return The number of failed rounds executed prior to adding the block to the chain.
   */
  int getRoundNumberFrom(final BlockHeader header);

  /**
   * Extracts the collection of signers from the supplied block header and returns them to the
   * caller.
   *
   * @param header the block header from which a list of signers is to be extracted
   * @return The addresses of
   */
  Collection<Address> getSignersFrom(final BlockHeader header);

  /**
   * Returns the literal name of the BFT consensus mechanism is use (eg ibft or qbft), which forms
   * the prefix for all BFT metrics.
   *
   * @return The name of the consensus mechanism being used by Besu
   */
  String getConsensusMechanismName();
}
