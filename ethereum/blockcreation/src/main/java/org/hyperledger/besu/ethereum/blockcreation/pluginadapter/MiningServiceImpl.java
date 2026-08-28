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
package org.hyperledger.besu.ethereum.blockcreation.pluginadapter;

import org.hyperledger.besu.ethereum.blockcreation.MiningCoordinator;
import org.hyperledger.besu.plugin.services.mining.MiningService;

/**
 * Implementation of the {@link MiningService} interface. This class provides methods to start and
 * stop the mining process using a {@link MiningCoordinator}.
 */
@SuppressWarnings("removal") // MiningService is deprecated for removal; the adapter goes with it
public class MiningServiceImpl implements MiningService {

  private final MiningCoordinator miningCoordinator;

  /**
   * Constructs a new {@code MiningServiceImpl} with the specified {@link MiningCoordinator}.
   *
   * @param miningCoordinator the mining coordinator to be used for starting and stopping the mining
   *     process
   */
  public MiningServiceImpl(final MiningCoordinator miningCoordinator) {
    this.miningCoordinator = miningCoordinator;
  }

  /** Stops the mining process by delegating to the {@link MiningCoordinator}. */
  @Override
  public void stop() {
    miningCoordinator.stop();
  }

  /** Starts the mining process by delegating to the {@link MiningCoordinator}. */
  @Override
  public void start() {
    miningCoordinator.start();
  }
}
