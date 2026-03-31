/*
 * Copyright 2020 ConsenSys AG.
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

import org.hyperledger.besu.config.GenesisConfigOptions;
import org.hyperledger.besu.ethereum.core.MiningConfiguration;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.plugin.services.MetricsSystem;

import java.math.BigInteger;
import java.util.Optional;

public class MainnetProtocolSpecFactory {

  private final Optional<BigInteger> chainId;
  private final boolean isRevertReasonEnabled;
  private final GenesisConfigOptions genesisConfigOptions;
  private final EvmConfiguration evmConfiguration;
  private final MiningConfiguration miningConfiguration;
  private final boolean isParallelTxProcessingEnabled;
  private final BalConfiguration balConfiguration;
  private final MetricsSystem metricsSystem;

  public MainnetProtocolSpecFactory(
      final Optional<BigInteger> chainId,
      final boolean isRevertReasonEnabled,
      final GenesisConfigOptions genesisConfigOptions,
      final EvmConfiguration evmConfiguration,
      final MiningConfiguration miningConfiguration,
      final boolean isParallelTxProcessingEnabled,
      final BalConfiguration balConfiguration,
      final MetricsSystem metricsSystem) {
    this.chainId = chainId;
    this.isRevertReasonEnabled = isRevertReasonEnabled;
    this.genesisConfigOptions = genesisConfigOptions;
    this.evmConfiguration = evmConfiguration;
    this.miningConfiguration = miningConfiguration;
    this.isParallelTxProcessingEnabled = isParallelTxProcessingEnabled;
    this.balConfiguration = balConfiguration;
    this.metricsSystem = metricsSystem;
  }

  public ProtocolSpecBuilder frontierDefinition() {
    return MainnetProtocolSpecs.frontierDefinition(
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder homesteadDefinition() {
    return MainnetProtocolSpecs.homesteadDefinition(
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder daoRecoveryInitDefinition() {
    return MainnetProtocolSpecs.daoRecoveryInitDefinition(
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder daoRecoveryTransitionDefinition() {
    return MainnetProtocolSpecs.daoRecoveryTransitionDefinition(
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder tangerineWhistleDefinition() {
    return MainnetProtocolSpecs.tangerineWhistleDefinition(
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder spuriousDragonDefinition() {
    return MainnetProtocolSpecs.spuriousDragonDefinition(
        chainId,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder byzantiumDefinition() {
    return MainnetProtocolSpecs.byzantiumDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder constantinopleDefinition() {
    return MainnetProtocolSpecs.constantinopleDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder petersburgDefinition() {
    return MainnetProtocolSpecs.petersburgDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder istanbulDefinition() {
    return MainnetProtocolSpecs.istanbulDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder muirGlacierDefinition() {
    return MainnetProtocolSpecs.muirGlacierDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder berlinDefinition() {
    return MainnetProtocolSpecs.berlinDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder londonDefinition() {
    return MainnetProtocolSpecs.londonDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder arrowGlacierDefinition() {
    return MainnetProtocolSpecs.arrowGlacierDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder grayGlacierDefinition() {
    return MainnetProtocolSpecs.grayGlacierDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder parisDefinition() {
    return MainnetProtocolSpecs.parisDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder shanghaiDefinition() {
    return MainnetProtocolSpecs.shanghaiDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder cancunDefinition() {
    return MainnetProtocolSpecs.cancunDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder pragueDefinition() {
    return MainnetProtocolSpecs.pragueDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder osakaDefinition() {
    return MainnetProtocolSpecs.osakaDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder binTrieDefinition() {
    return MainnetProtocolSpecs.binTrieDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder bpo1Definition() {
    return MainnetProtocolSpecs.bpo1Definition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder bpo2Definition() {
    return MainnetProtocolSpecs.bpo2Definition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder bpo3Definition() {
    return MainnetProtocolSpecs.bpo3Definition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder bpo4Definition() {
    return MainnetProtocolSpecs.bpo4Definition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder bpo5Definition() {
    return MainnetProtocolSpecs.bpo5Definition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  public ProtocolSpecBuilder amsterdamDefinition() {
    return MainnetProtocolSpecs.amsterdamDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  /**
   * The "future" fork consists of EIPs that have been approved for Ethereum Mainnet but not
   * scheduled for a fork. This is also known as "Eligible For Inclusion" (EFI) or "Considered for
   * Inclusion" (CFI).
   *
   * <p>There is no guarantee of the contents of this fork across Besu releases and should be
   * considered unstable.
   *
   * @return a protocol spec for the "Future" fork.
   */
  public ProtocolSpecBuilder futureEipsDefinition() {
    return MainnetProtocolSpecs.futureEipsDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }

  /**
   * The "experimental" fork consists of EIPs and other changes that have not been approved for any
   * fork but are implemented in Besu, either for demonstration or experimentation.
   *
   * <p>There is no guarantee of the contents of this fork across Besu releases and should be
   * considered unstable.
   *
   * @return a protocol spec for the "Experimental" fork.
   */
  public ProtocolSpecBuilder experimentalEipsDefinition() {
    return MainnetProtocolSpecs.experimentalEipsDefinition(
        chainId,
        isRevertReasonEnabled,
        genesisConfigOptions,
        evmConfiguration,
        miningConfiguration,
        isParallelTxProcessingEnabled,
        balConfiguration,
        metricsSystem);
  }
}
