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
package org.hyperledger.besu.ethereum;

import org.hyperledger.besu.ethereum.chain.BadBlockManager;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.worldstate.WorldStateArchive;
import org.hyperledger.besu.plugin.ServiceManager;

import java.util.Optional;

/**
 * Holds the mutable state used to track the current context of the protocol. This is primarily the
 * blockchain and world state archive, but can also hold arbitrary context required by a particular
 * consensus algorithm.
 */
public class ProtocolContext {
  private final MutableBlockchain blockchain;
  private final WorldStateArchive worldStateArchive;
  private final ConsensusContext consensusContext;
  private final BadBlockManager badBlockManager;
  private final ServiceManager serviceManager;
  private final long slowBlockThresholdMs;

  /**
   * Constructs a new ProtocolContext with the given blockchain, world state archive, consensus
   * context, and bad block manager.
   *
   * @param blockchain the blockchain of the protocol context
   * @param worldStateArchive the world state archive of the protocol context
   * @param consensusContext the consensus context
   * @param badBlockManager the bad block manager of the protocol context
   * @param serviceManager plugin service manager
   * @param slowBlockThresholdMs threshold in ms for slow block logging (-1 = disabled)
   */
  protected ProtocolContext(
      final MutableBlockchain blockchain,
      final WorldStateArchive worldStateArchive,
      final ConsensusContext consensusContext,
      final BadBlockManager badBlockManager,
      final ServiceManager serviceManager,
      final long slowBlockThresholdMs) {
    this.blockchain = blockchain;
    this.worldStateArchive = worldStateArchive;
    this.consensusContext = consensusContext;
    this.badBlockManager = badBlockManager;
    this.serviceManager = serviceManager;
    this.slowBlockThresholdMs = slowBlockThresholdMs;
  }

  /**
   * Gets the blockchain of the protocol context.
   *
   * @return the blockchain of the protocol context
   */
  public MutableBlockchain getBlockchain() {
    return blockchain;
  }

  /**
   * Gets the world state archive of the protocol context.
   *
   * @return the world state archive of the protocol context
   */
  public WorldStateArchive getWorldStateArchive() {
    return worldStateArchive;
  }

  /**
   * Gets the bad block manager of the protocol context.
   *
   * @return the bad block manager of the protocol context
   */
  public BadBlockManager getBadBlockManager() {
    return badBlockManager;
  }

  /**
   * Gets the plugin service manager from protocol context.
   *
   * @return the serviceManager manager from protocol context
   */
  public ServiceManager getPluginServiceManager() {
    return serviceManager;
  }

  /**
   * Gets the slow block threshold in milliseconds. Negative means disabled, 0 means log all blocks,
   * positive means only log blocks exceeding this threshold.
   *
   * @return the slow block threshold in milliseconds
   */
  public long getSlowBlockThresholdMs() {
    return slowBlockThresholdMs;
  }

  /**
   * Gets the consensus context of the protocol context.
   *
   * @param <C> the type of the consensus context
   * @param klass the klass
   * @return the consensus context of the protocol context
   */
  public <C extends ConsensusContext> C getConsensusContext(final Class<C> klass) {
    return consensusContext.as(klass);
  }

  /**
   * Gets the consensus context of the protocol context.
   *
   * @param <C> the type of the consensus context
   * @param klass the klass
   * @param blockNumber the block number
   * @return the consensus context of the protocol context
   */
  public <C extends ConsensusContext> C getConsensusContext(
      final Class<C> klass, final long blockNumber) {
    return consensusContext.as(klass);
  }

  /**
   * Gets the safe consensus context of the protocol context.
   *
   * @param <C> the type of the consensus context
   * @param klass the klass
   * @return the consensus context of the protocol context
   */
  public <C extends ConsensusContext> Optional<C> safeConsensusContext(final Class<C> klass) {
    return Optional.ofNullable(consensusContext)
        .filter(c -> klass.isAssignableFrom(c.getClass()))
        .map(klass::cast);
  }

  /**
   * Builder for creating instances of {@link ProtocolContext}. This builder class follows the
   * builder pattern to provide a flexible and clear way to construct {@link ProtocolContext}
   * objects with potentially complex configurations.
   *
   * <p>Usage example:
   *
   * <pre>
   * ProtocolContext protocolContext = new ProtocolContext.Builder()
   *     .withBlockchain(new MutableBlockchainImpl())
   *     .withWorldStateArchive(new WorldStateArchiveImpl())
   *     .withConsensusContext(new ConsensusContextImpl())
   *     .withBadBlockManager(new BadBlockManagerImpl())
   *     .withServiceManager(new ServiceManager.SimpleServiceManager())
   *     .build();
   * </pre>
   */
  public static class Builder {
    private MutableBlockchain blockchain;
    private WorldStateArchive worldStateArchive;
    private ConsensusContext consensusContext;
    private BadBlockManager badBlockManager = new BadBlockManager();
    private ServiceManager serviceManager = new ServiceManager.SimpleServiceManager();
    private long slowBlockThresholdMs = -1L;

    /** Default constructor. linter requires javadoc. */
    public Builder() {}

    /**
     * Sets the {@link MutableBlockchain} for the {@link ProtocolContext}.
     *
     * @param blockchain the blockchain to be used in the protocol context.
     * @return the builder instance for chaining.
     */
    public Builder withBlockchain(final MutableBlockchain blockchain) {
      this.blockchain = blockchain;
      return this;
    }

    /**
     * Sets the {@link WorldStateArchive} for the {@link ProtocolContext}.
     *
     * @param worldStateArchive the world state archive to be used in the protocol context.
     * @return the builder instance for chaining.
     */
    public Builder withWorldStateArchive(final WorldStateArchive worldStateArchive) {
      this.worldStateArchive = worldStateArchive;
      return this;
    }

    /**
     * Sets the {@link ConsensusContext} for the {@link ProtocolContext}.
     *
     * @param consensusContext the consensus context to be used in the protocol context.
     * @return the builder instance for chaining.
     */
    public Builder withConsensusContext(final ConsensusContext consensusContext) {
      this.consensusContext = consensusContext;
      return this;
    }

    /**
     * Sets the {@link BadBlockManager} for the {@link ProtocolContext}.
     *
     * @param badBlockManager the bad block manager to be used in the protocol context.
     * @return the builder instance for chaining.
     */
    public Builder withBadBlockManager(final BadBlockManager badBlockManager) {
      this.badBlockManager = badBlockManager;
      return this;
    }

    /**
     * Sets the {@link ServiceManager} for the {@link ProtocolContext}.
     *
     * @param serviceManager the service manager to be used in the protocol context.
     * @return the builder instance for chaining.
     */
    public Builder withServiceManager(final ServiceManager serviceManager) {
      this.serviceManager = serviceManager;
      return this;
    }

    /**
     * Sets the slow block threshold in milliseconds for the {@link ProtocolContext}. Negative means
     * disabled, 0 means log all blocks, positive means only log blocks exceeding this threshold.
     *
     * @param slowBlockThresholdMs the threshold in milliseconds.
     * @return the builder instance for chaining.
     */
    public Builder withSlowBlockThreshold(final long slowBlockThresholdMs) {
      this.slowBlockThresholdMs = slowBlockThresholdMs;
      return this;
    }

    /**
     * Constructs a new {@link ProtocolContext} using the currently configured properties.
     *
     * @return a new {@link ProtocolContext} instance with the specified properties.
     */
    public ProtocolContext build() {
      return new ProtocolContext(
          blockchain,
          worldStateArchive,
          consensusContext,
          badBlockManager,
          serviceManager,
          slowBlockThresholdMs);
    }
  }
}
