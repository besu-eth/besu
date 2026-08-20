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
package org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview;

/** WorldStateConfig encapsulates the shared configuration parameters for the world state. */
public class WorldStateConfig {

  /**
   * Indicates whether the trie is disabled for the world state. When the trie is disabled, the
   * world state will only work with the flat database and not the trie. In this mode, it's
   * impossible to verify the state root.
   */
  private boolean isTrieDisabled;

  /**
   * Indicates whether parallel processing is enabled for state root calculation. When enabled, the
   * trie operations during root hash computation are parallelized. Default is true
   */
  private boolean isParallelStateRootComputationEnabled;

  private WorldStateConfig(final Builder builder) {
    this.isTrieDisabled = builder.isTrieDisabled;
    this.isParallelStateRootComputationEnabled = builder.isParallelStateRootComputationEnabled;
  }

  public boolean isTrieDisabled() {
    return isTrieDisabled;
  }

  public boolean isParallelStateRootComputationEnabled() {
    return isParallelStateRootComputationEnabled;
  }

  public void setTrieDisabled(final boolean trieDisabled) {
    isTrieDisabled = trieDisabled;
  }

  public void setParallelStateRootComputationEnabled(
      final boolean parallelStateRootComputationEnabled) {
    isParallelStateRootComputationEnabled = parallelStateRootComputationEnabled;
  }

  /**
   * Merges this WorldStateConfig with another WorldStateConfig and returns a new instance.
   *
   * @param other the other WorldStateConfig to merge with
   * @return a new WorldStateConfig instance with merged values
   */
  public WorldStateConfig apply(final WorldStateConfig other) {
    return new Builder(this)
        .trieDisabled(other.isTrieDisabled)
        .parallelStateRootComputationEnabled(other.isParallelStateRootComputationEnabled)
        .build();
  }

  public static Builder newBuilder() {
    return new Builder();
  }

  public static Builder newBuilder(final WorldStateConfig worldStateConfig) {
    return new Builder(worldStateConfig);
  }

  public static WorldStateConfig createStatefulConfigWithTrie() {
    return newBuilder().parallelStateRootComputationEnabled(true).trieDisabled(false).build();
  }

  public static class Builder {
    private boolean isTrieDisabled = false;
    private boolean isParallelStateRootComputationEnabled = true;

    public Builder() {}

    public Builder(final WorldStateConfig spec) {
      this.isTrieDisabled = spec.isTrieDisabled();
      this.isParallelStateRootComputationEnabled = spec.isParallelStateRootComputationEnabled();
    }

    public Builder trieDisabled(final boolean trieDisabled) {
      this.isTrieDisabled = trieDisabled;
      return this;
    }

    public Builder parallelStateRootComputationEnabled(
        final boolean parallelStateRootComputationEnabled) {
      this.isParallelStateRootComputationEnabled = parallelStateRootComputationEnabled;
      return this;
    }

    public WorldStateConfig build() {
      return new WorldStateConfig(this);
    }
  }
}
