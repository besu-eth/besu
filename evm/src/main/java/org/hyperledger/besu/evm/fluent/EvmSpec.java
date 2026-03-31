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
package org.hyperledger.besu.evm.fluent;

import static com.google.common.base.Preconditions.checkNotNull;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.EVM;
import org.hyperledger.besu.evm.EvmSpecVersion;
import org.hyperledger.besu.evm.MainnetEVMs;
import org.hyperledger.besu.evm.contractvalidation.ContractValidationRule;
import org.hyperledger.besu.evm.contractvalidation.MaxCodeSizeRule;
import org.hyperledger.besu.evm.contractvalidation.PrefixCodeRule;
import org.hyperledger.besu.evm.internal.EvmConfiguration;
import org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts;
import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;

import java.math.BigInteger;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

import com.google.errorprone.annotations.InlineMe;
import org.apache.tuweni.bytes.Bytes;

/** Specification for the EVM execution. */
public final class EvmSpec {
  private final EVM evm;
  private PrecompileContractRegistry precompileContractRegistry;
  private List<ContractValidationRule> contractValidationRules =
      List.of(
          MaxCodeSizeRule.from(EvmSpecVersion.SPURIOUS_DRAGON, EvmConfiguration.DEFAULT),
          PrefixCodeRule.of());
  private long initialNonce = 1;
  private boolean requireDeposit = true;
  private Collection<Address> forceCommitAddresses = List.of(Address.fromHexString("0x03"));

  private EvmSpec(final EVM evm) {
    this.evm = evm;
  }

  /**
   * Create an EVM spec with the most current activated fork on chain ID 1.
   *
   * <p>Note, this will change across versions
   *
   * @return evm spec builder
   */
  public static EvmSpec evmSpec() {
    return evmSpec(EvmSpecVersion.mostRecent());
  }

  /**
   * Create an EVM spec at the specified version with chain ID 1.
   *
   * @param fork the EVM spec version to use
   * @return evm spec builder
   */
  public static EvmSpec evmSpec(final EvmSpecVersion fork) {
    return evmSpec(fork, BigInteger.ONE);
  }

  /**
   * Create an EVM spec at the specified version and chain ID
   *
   * @param fork the EVM spec version to use
   * @param chainId the chain ID to use
   * @return evm spec builder
   */
  public static EvmSpec evmSpec(final EvmSpecVersion fork, final BigInteger chainId) {
    return evmSpec(fork, chainId, EvmConfiguration.DEFAULT);
  }

  /**
   * Create an EVM spec at the specified version and chain ID
   *
   * @param fork the EVM spec version to use
   * @param chainId the chain ID to use
   * @return evm spec builder
   */
  public static EvmSpec evmSpec(final EvmSpecVersion fork, final Bytes chainId) {
    return evmSpec(fork, new BigInteger(1, chainId.toArrayUnsafe()), EvmConfiguration.DEFAULT);
  }

  /**
   * Create an EVM spec at the specified version and chain ID
   *
   * @param fork the EVM spec version to use
   * @param chainId the chain ID to use
   * @param evmConfiguration system configuration options.
   * @return evm spec builder
   */
  public static EvmSpec evmSpec(
      final EvmSpecVersion fork,
      final BigInteger chainId,
      final EvmConfiguration evmConfiguration) {
    return switch (fork) {
      case FRONTIER -> frontier(evmConfiguration);
      case HOMESTEAD -> homestead(evmConfiguration);
      case TANGERINE_WHISTLE -> tangerineWhistle(evmConfiguration);
      case SPURIOUS_DRAGON -> spuriousDragon(evmConfiguration);
      case BYZANTIUM -> byzantium(evmConfiguration);
      case CONSTANTINOPLE -> constantinople(evmConfiguration);
      case PETERSBURG -> petersburg(evmConfiguration);
      case ISTANBUL -> istanbul(chainId, evmConfiguration);
      case BERLIN -> berlin(chainId, evmConfiguration);
      case LONDON -> london(chainId, evmConfiguration);
      case PARIS -> paris(chainId, evmConfiguration);
      case SHANGHAI -> shanghai(chainId, evmConfiguration);
      case CANCUN -> cancun(chainId, evmConfiguration);
      case PRAGUE -> prague(chainId, evmConfiguration);
      case OSAKA -> osaka(chainId, evmConfiguration);
      case BINTRIE -> binTrie(chainId, evmConfiguration);
      case AMSTERDAM -> amsterdam(chainId, evmConfiguration);
      case BOGOTA -> bogota(chainId, evmConfiguration);
      case POLIS -> polis(chainId, evmConfiguration);
      case BANGKOK -> bangkok(chainId, evmConfiguration);
      case FUTURE_EIPS -> futureEips(chainId, evmConfiguration);
      case EXPERIMENTAL_EIPS -> experimentalEips(chainId, evmConfiguration);
    };
  }

  /**
   * Instantiate Evm spec.
   *
   * @param evm the evm
   * @return the evm spec
   */
  public static EvmSpec evmSpec(final EVM evm) {
    checkNotNull(evm, "evm must not be null");
    return new EvmSpec(evm);
  }

  /**
   * Instantiate Frontier evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec frontier(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.frontier(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.frontier(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of();
    evmSpec.requireDeposit = false;
    evmSpec.forceCommitAddresses = List.of();
    evmSpec.initialNonce = 0;
    return evmSpec;
  }

  /**
   * Instantiate Homestead evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec homestead(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.homestead(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.frontier(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of();
    evmSpec.forceCommitAddresses = List.of();
    evmSpec.initialNonce = 0;
    return evmSpec;
  }

  /**
   * Instantiate Tangerine whistle evm evmSpec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec tangerineWhistle(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.tangerineWhistle(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.frontier(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of();
    evmSpec.initialNonce = 0;
    return evmSpec;
  }

  /**
   * Instantiate Spurious dragon evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec spuriousDragon(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.spuriousDragon(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.frontier(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate Byzantium evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec byzantium(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.byzantium(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.byzantium(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate Constantinople evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec constantinople(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.constantinople(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.byzantium(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate Petersburg evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec petersburg(final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.petersburg(evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.byzantium(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate Istanbul evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.ISTANBUL, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec istanbul(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.ISTANBUL, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate Istanbul evm evmSpec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec istanbul(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.istanbul(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.istanbul(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate Berlin evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.BERLIN, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec berlin(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.BERLIN, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate berlin evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec berlin(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.berlin(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.istanbul(evmSpec.evm.getGasCalculator());
    evmSpec.contractValidationRules = List.of(MaxCodeSizeRule.from(evmSpec.evm));
    return evmSpec;
  }

  /**
   * Instantiate London evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.LONDON, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec london(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.LONDON, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate London evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec london(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.london(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.istanbul(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Paris evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.PARIS, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec paris(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.PARIS, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate Paris evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec paris(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.paris(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.istanbul(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Shanghai evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.SHANGHAI, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec shanghai(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.SHANGHAI, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate Shanghai evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec shanghai(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.shanghai(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.istanbul(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Cancun evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.CANCUN, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec cancun(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.CANCUN, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate Cancun evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec cancun(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.cancun(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.cancun(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Prague evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec prague(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.prague(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Osaka evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec osaka(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.osaka(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.osaka(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate BinTrie evm spec - extends Prague with Binary Trie stateless gas costs (EIP-4762).
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec binTrie(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.binTrie(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Amsterdam evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec amsterdam(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.amsterdam(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Bogota evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec bogota(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.bogota(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Polis evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec polis(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.polis(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Bangkok evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec bangkok(final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.bangkok(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.prague(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Future EIPs evm spec.
   *
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   * @deprecated Migrate to use {@link EvmSpec#evmSpec(EvmSpecVersion)}.
   */
  @SuppressWarnings("DeprecatedIsStillUsed")
  @InlineMe(
      replacement = "EvmSpec.evmSpec(EvmSpecVersion.FUTURE_EIPS, BigInteger.ONE, evmConfiguration)",
      imports = {
        "java.math.BigInteger",
        "org.hyperledger.besu.evm.EvmSpecVersion",
        "org.hyperledger.besu.evm.fluent.EvmSpec"
      })
  @Deprecated(forRemoval = true)
  public static EvmSpec futureEips(final EvmConfiguration evmConfiguration) {
    return evmSpec(EvmSpecVersion.FUTURE_EIPS, BigInteger.ONE, evmConfiguration);
  }

  /**
   * Instantiate Future EIPs evm evmSpec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec futureEips(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.futureEips(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.futureEIPs(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Instantiate Experimental EIPs evm spec.
   *
   * @param chainId the chain ID
   * @param evmConfiguration the evm configuration
   * @return the evm spec
   */
  public static EvmSpec experimentalEips(
      final BigInteger chainId, final EvmConfiguration evmConfiguration) {
    final EvmSpec evmSpec = new EvmSpec(MainnetEVMs.experimentalEips(chainId, evmConfiguration));
    evmSpec.precompileContractRegistry =
        MainnetPrecompiledContracts.futureEIPs(evmSpec.evm.getGasCalculator());
    return evmSpec;
  }

  /**
   * Sets Precompile contract registry.
   *
   * @param precompileContractRegistry the precompile contract registry
   * @return the evm spec
   */
  public EvmSpec precompileContractRegistry(
      final PrecompileContractRegistry precompileContractRegistry) {
    this.precompileContractRegistry = precompileContractRegistry;
    return this;
  }

  /**
   * Sets Contract validation rules.
   *
   * @param contractValidationRules the contract validation rules
   * @return the evm spec
   */
  public EvmSpec contractValidationRules(
      final List<ContractValidationRule> contractValidationRules) {
    this.contractValidationRules = contractValidationRules;
    return this;
  }

  /**
   * Sets Initial nonce.
   *
   * @param initialNonce the initial nonce
   * @return the evm spec
   */
  public EvmSpec initialNonce(final long initialNonce) {
    this.initialNonce = initialNonce;
    return this;
  }

  /**
   * Sets Require deposit.
   *
   * @param requireDeposit the require deposit
   * @return the evm spec
   */
  public EvmSpec requireDeposit(final boolean requireDeposit) {
    this.requireDeposit = requireDeposit;
    return this;
  }

  /**
   * List of EIP-718 contracts that require special delete handling. By default, this is only the
   * RIPEMD precompile contract.
   *
   * @param forceCommitAddresses collection of addresses for special handling
   * @return fluent executor
   * @see <a
   *     href="https://github.com/ethereum/EIPs/issues/716">https://github.com/ethereum/EIPs/issues/716</a>
   */
  public EvmSpec forceCommitAddresses(final Collection<Address> forceCommitAddresses) {
    this.forceCommitAddresses = forceCommitAddresses;
    return this;
  }

  /**
   * The configured EVM.
   *
   * @return the EVM
   */
  public EVM getEvm() {
    return evm;
  }

  /**
   * Gets the configured precompile contract registry.
   *
   * @return the precompile contract registry
   */
  public PrecompileContractRegistry getPrecompileContractRegistry() {
    return precompileContractRegistry;
  }

  /**
   * Gets the configured contract validation rules.
   *
   * @return the contract validation rules
   */
  public List<ContractValidationRule> getContractValidationRules() {
    return contractValidationRules;
  }

  /**
   * Gets the configured initial nonce.
   *
   * @return the initial nonce
   */
  public long getInitialNonce() {
    return initialNonce;
  }

  /**
   * Checks if deposit required is configured.
   *
   * @return if deposit is required
   */
  public boolean isRequireDeposit() {
    return requireDeposit;
  }

  /**
   * Gets the list of EIP-718 contracts that require special delete handling. By default, this is
   * only the RIPEMD precompile contract.
   *
   * @return collection of addresses for special handling
   * @see <a
   *     href="https://github.com/ethereum/EIPs/issues/716">https://github.com/ethereum/EIPs/issues/716</a>
   */
  public Collection<Address> getForceCommitAddresses() {
    return forceCommitAddresses;
  }

  /**
   * Returns the EVM version
   *
   * @return the current EVM version
   */
  public EvmSpecVersion getEVMVersion() {
    return evm.getEvmVersion();
  }

  /**
   * Returns the ChainID this evm spec is using
   *
   * @return the current chain ID
   */
  public Optional<Bytes> getChainId() {
    return evm.getChainId();
  }
}
