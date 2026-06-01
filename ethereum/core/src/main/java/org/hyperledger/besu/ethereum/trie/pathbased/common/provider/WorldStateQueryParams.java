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
package org.hyperledger.besu.ethereum.trie.pathbased.common.provider;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.BlockAccessList;
import org.hyperledger.besu.plugin.data.BlockHeader;

import java.util.Objects;
import java.util.Optional;

/** Parameters for querying the world state. */
public class WorldStateQueryParams {

  /**
   * BAL overlay applied when the world state's accumulator is created (parallel execution).
   *
   * @param blockAccessList block access list for the current block
   * @param maxTxIndexExclusive index of the transaction about to execute; overlay includes changes
   *     with {@code txIndex < maxTxIndexExclusive}
   */
  public record BalOverlayQuery(BlockAccessList blockAccessList, long maxTxIndexExclusive) {}

  private final BlockHeader blockHeader;
  private final boolean shouldWorldStateUpdateHead;
  private final Hash blockHash;
  private final Optional<Hash> stateRoot;
  private final Optional<BalOverlayQuery> balOverlayQuery;

  /**
   * Private constructor to enforce the use of the Builder.
   *
   * @param builder the builder to create an instance of WorldStateQueryParams
   */
  private WorldStateQueryParams(final Builder builder) {
    this.blockHeader = builder.blockHeader;
    this.shouldWorldStateUpdateHead = builder.shouldWorldStateUpdateHead;
    this.blockHash = builder.blockHash;
    this.stateRoot = builder.stateRoot;
    this.balOverlayQuery = builder.balOverlayQuery;
  }

  /**
   * Gets the block header.
   *
   * @return the block header
   */
  public BlockHeader getBlockHeader() {
    return blockHeader;
  }

  /**
   * Checks if the world state should update the node head.
   *
   * @return true if the world state should update the node head, false otherwise
   */
  public boolean shouldWorldStateUpdateHead() {
    return shouldWorldStateUpdateHead;
  }

  /**
   * Gets the block hash.
   *
   * @return the block hash
   */
  public Hash getBlockHash() {
    return blockHash;
  }

  /**
   * Gets the state root.
   *
   * @return the state root
   */
  public Optional<Hash> getStateRoot() {
    return stateRoot;
  }

  /**
   * Optional BAL overlay to configure on the new world state's accumulator at creation time.
   *
   * @return BAL overlay query parameters, if any
   */
  public Optional<BalOverlayQuery> getBalOverlayQuery() {
    return balOverlayQuery;
  }

  /**
   * Creates a new builder for WorldStateQueryParams.
   *
   * @return a new builder
   */
  public static Builder newBuilder() {
    return new Builder();
  }

  /**
   * Creates an instance with a block header and updates the node head.
   *
   * @param blockHeader the block header
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndUpdateNodeHead(
      final BlockHeader blockHeader) {
    return newBuilder().withBlockHeader(blockHeader).withShouldWorldStateUpdateHead(true).build();
  }

  /**
   * Creates an instance with a block header and does not update the node head.
   *
   * @param blockHeader the block header
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withBlockHeaderAndNoUpdateNodeHead(
      final BlockHeader blockHeader) {
    return newBuilder().withBlockHeader(blockHeader).withShouldWorldStateUpdateHead(false).build();
  }

  /**
   * Should return a worldstate instance with a state root, block hash, and should update the node
   * head.
   *
   * @param stateRoot the state root
   * @param blockHash the block hash
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndBlockHashAndUpdateNodeHead(
      final Hash stateRoot, final Hash blockHash) {
    return newBuilder()
        .withStateRoot(stateRoot)
        .withBlockHash(blockHash)
        .withShouldWorldStateUpdateHead(true)
        .build();
  }

  /**
   * Should return a worldstate instance with a state root and should update the node head.
   *
   * @param stateRoot the state root
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndUpdateNodeHead(final Hash stateRoot) {
    return newBuilder().withStateRoot(stateRoot).withShouldWorldStateUpdateHead(true).build();
  }

  /**
   * Creates an instance with a state root, block hash, and does not update the node head.
   *
   * @param stateRoot the state root
   * @param blockHash the block hash
   * @return an instance of WorldStateQueryParams
   */
  public static WorldStateQueryParams withStateRootAndBlockHashAndNoUpdateNodeHead(
      final Hash stateRoot, final Hash blockHash) {
    return newBuilder()
        .withStateRoot(stateRoot)
        .withBlockHash(blockHash)
        .withShouldWorldStateUpdateHead(false)
        .build();
  }

  @Override
  public boolean equals(final Object o) {
    if (o == null || getClass() != o.getClass()) return false;
    WorldStateQueryParams that = (WorldStateQueryParams) o;
    return shouldWorldStateUpdateHead == that.shouldWorldStateUpdateHead
        && Objects.equals(blockHeader, that.blockHeader)
        && Objects.equals(blockHash, that.blockHash)
        && Objects.equals(stateRoot, that.stateRoot)
        && Objects.equals(balOverlayQuery, that.balOverlayQuery);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        blockHeader, shouldWorldStateUpdateHead, blockHash, stateRoot, balOverlayQuery);
  }

  public static class Builder {
    private BlockHeader blockHeader;
    private boolean shouldWorldStateUpdateHead = false;
    private Hash blockHash;
    private Optional<Hash> stateRoot = Optional.empty();
    private Optional<BalOverlayQuery> balOverlayQuery = Optional.empty();

    private Builder() {}

    /**
     * Sets the block header.
     *
     * @param blockHeader the block header
     * @return the builder
     */
    public Builder withBlockHeader(final BlockHeader blockHeader) {
      this.blockHeader = blockHeader;
      this.blockHash = blockHeader.getBlockHash();
      this.stateRoot = Optional.of(blockHeader.getStateRoot());
      return this;
    }

    /**
     * Sets whether the world state should update the node head.
     *
     * @param shouldWorldStateUpdateHead true if the world state should update the node head, false
     *     otherwise
     * @return the builder
     */
    public Builder withShouldWorldStateUpdateHead(final boolean shouldWorldStateUpdateHead) {
      this.shouldWorldStateUpdateHead = shouldWorldStateUpdateHead;
      return this;
    }

    /**
     * Sets the block hash.
     *
     * @param blockHash the block hash
     * @return the builder
     */
    public Builder withBlockHash(final Hash blockHash) {
      this.blockHash = blockHash;
      return this;
    }

    /**
     * Sets the state root.
     *
     * @param stateRoot the state root
     * @return the builder
     */
    public Builder withStateRoot(final Hash stateRoot) {
      this.stateRoot = Optional.ofNullable(stateRoot);
      return this;
    }

    /**
     * Applies a BAL overlay when the queried world state's accumulator is created.
     *
     * @param blockAccessList block access list for the current block
     * @param maxTxIndexExclusive index of the transaction about to execute
     * @return the builder
     */
    public Builder withBalOverlay(
        final BlockAccessList blockAccessList, final long maxTxIndexExclusive) {
      this.balOverlayQuery = Optional.of(new BalOverlayQuery(blockAccessList, maxTxIndexExclusive));
      return this;
    }

    /**
     * Builds an instance of WorldStateQueryParams.
     *
     * @return an instance of WorldStateQueryParams
     */
    public WorldStateQueryParams build() {

      if (blockHash == null && stateRoot.isEmpty() && blockHeader == null) {
        throw new IllegalArgumentException(
            "Either blockHash, stateRoot, or blockHeader must be provided");
      }

      return new WorldStateQueryParams(this);
    }
  }
}
