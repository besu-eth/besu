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
package org.hyperledger.besu.components;

import org.hyperledger.besu.cli.BesuCommand;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCache;
import org.hyperledger.besu.ethereum.eth.transactions.BlobCacheModule;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoader;
import org.hyperledger.besu.ethereum.trie.pathbased.bonsai.worldview.accumulator.preload.BonsaiCachedMerkleTrieLoaderModule;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCache;
import org.hyperledger.besu.ethereum.trie.pathbased.common.code.PathBasedCodeCacheModule;
import org.hyperledger.besu.metrics.MetricsSystemModule;
import org.hyperledger.besu.plugin.services.MetricsSystem;
import org.hyperledger.besu.services.BesuPluginContextImpl;

import javax.inject.Named;
import javax.inject.Singleton;

import dagger.Component;
import org.slf4j.Logger;

/** An application context that knows how to provide dependencies based on Dagger setup. */
@Singleton
@Component(
    modules = {
      BesuCommandModule.class,
      MetricsSystemModule.class,
      BonsaiCachedMerkleTrieLoaderModule.class,
      BesuPluginContextModule.class,
      BlobCacheModule.class,
      PathBasedCodeCacheModule.class
    })
public interface BesuComponent {

  /**
   * the configured and parsed representation of the user issued command to run Besu
   *
   * @return BesuCommand
   */
  BesuCommand getBesuCommand();

  /**
   * a cached trie node loader
   *
   * @return CachedMerkleTrieLoader
   */
  BonsaiCachedMerkleTrieLoader getCachedMerkleTrieLoader();

  /**
   * a metrics system that is observable by a Prometheus or OTEL metrics collection subsystem
   *
   * @return ObservableMetricsSystem
   */
  MetricsSystem getMetricsSystem();

  /**
   * a Logger specifically configured to provide configuration feedback to users.
   *
   * @return Logger
   */
  @Named("besuCommandLogger")
  Logger getBesuCommandLogger();

  /**
   * Besu plugin context for doing plugin service discovery.
   *
   * @return BesuPluginContextImpl
   */
  BesuPluginContextImpl getBesuPluginContext();

  /**
   * Cache to store blobs in for re-use after reorgs.
   *
   * @return BlobCache
   */
  BlobCache getBlobCache();

  /**
   * Code cache for storing EVM code.
   *
   * @return PathBasedCodeCache
   */
  PathBasedCodeCache getCodeCache();
}
