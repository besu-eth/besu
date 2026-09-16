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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.PreprocessingFunction;
import org.hyperledger.besu.ethereum.mainnet.AbstractBlockProcessor.PreprocessingFunction.NoPreprocessing;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.services.worldstate.MutableWorldState;

import java.util.Objects;
import java.util.Optional;

import org.jspecify.annotations.Nullable;

/**
 * Encapsulates all inputs required to execute a block via {@link BlockProcessor#processBlock}.
 *
 * <p>The three core fields — {@code protocolContext}, {@code worldState}, and {@code block} — are
 * required. The blockchain is obtained from {@code protocolContext}. The remaining fields are
 * optional and default to no block access list and no preprocessing.
 *
 * <p>Construct instances via {@link #builder()}.
 */
public class BlockExecutionContext {

  private final ProtocolContext protocolContext;
  private final MutableWorldState worldState;
  private final Block block;
  @Nullable private final BlockAccessList blockAccessList;
  private final PreprocessingFunction preprocessingFunction;

  private BlockExecutionContext(final Builder builder) {
    this.protocolContext = builder.protocolContext;
    this.worldState = builder.worldState;
    this.block = builder.block;
    this.blockAccessList = builder.blockAccessList;
    this.preprocessingFunction = builder.preprocessingFunction;
  }

  /**
   * Returns the current protocol context.
   *
   * @return the current protocol context
   */
  public ProtocolContext getProtocolContext() {
    return protocolContext;
  }

  /**
   * Returns the mutable world state to apply changes to.
   *
   * @return the mutable world state to apply changes to
   */
  public MutableWorldState getWorldState() {
    return worldState;
  }

  /**
   * Returns the block to process.
   *
   * @return the block to process
   */
  public Block getBlock() {
    return block;
  }

  /**
   * Returns an optional pre-computed block access list to validate against.
   *
   * @return an optional pre-computed block access list to validate against, or empty to construct
   *     one during execution
   */
  public Optional<BlockAccessList> getBlockAccessList() {
    return Optional.ofNullable(blockAccessList);
  }

  /**
   * Returns the preprocessing function to run before transaction execution.
   *
   * @return the preprocessing function to run before transaction execution, or a no-op if not set
   */
  public PreprocessingFunction getPreprocessingFunction() {
    return preprocessingFunction;
  }

  /**
   * Returns a new {@link Builder}.
   *
   * @return a new {@link Builder}
   */
  public static Builder builder() {
    return new Builder();
  }

  /** Builder for {@link BlockExecutionContext}. */
  public static class Builder {

    private ProtocolContext protocolContext;
    private MutableWorldState worldState;
    private Block block;
    @Nullable private BlockAccessList blockAccessList = null;
    private PreprocessingFunction preprocessingFunction = new NoPreprocessing();

    /**
     * Sets the protocol context (required).
     *
     * @param protocolContext the current protocol context (required)
     * @return this builder
     */
    public Builder protocolContext(final ProtocolContext protocolContext) {
      this.protocolContext = protocolContext;
      return this;
    }

    /**
     * Sets the world state (required).
     *
     * @param worldState the world state to apply changes to (required)
     * @return this builder
     */
    public Builder worldState(final MutableWorldState worldState) {
      this.worldState = worldState;
      return this;
    }

    /**
     * Sets the block to process (required).
     *
     * @param block the block to process (required)
     * @return this builder
     */
    public Builder block(final Block block) {
      this.block = block;
      return this;
    }

    /**
     * Sets the optional pre-computed block access list.
     *
     * @param blockAccessList a pre-computed block access list to validate against (optional)
     * @return this builder
     */
    public Builder blockAccessList(final Optional<BlockAccessList> blockAccessList) {
      this.blockAccessList =
          Objects.requireNonNull(blockAccessList, "blockAccessList").orElse(null);
      return this;
    }

    /**
     * Sets the optional preprocessing function.
     *
     * @param preprocessingFunction a function to run before transaction execution (optional)
     * @return this builder
     */
    public Builder preprocessingFunction(final PreprocessingFunction preprocessingFunction) {
      this.preprocessingFunction =
          Objects.requireNonNull(preprocessingFunction, "preprocessingFunction");
      return this;
    }

    /**
     * Builds the {@link BlockExecutionContext}.
     *
     * @return the constructed context
     * @throws IllegalStateException if any required field is missing
     */
    public BlockExecutionContext build() {
      if (protocolContext == null) throw new IllegalStateException("protocolContext is required");
      if (worldState == null) throw new IllegalStateException("worldState is required");
      if (block == null) throw new IllegalStateException("block is required");
      return new BlockExecutionContext(this);
    }
  }
}
