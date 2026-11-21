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
package org.hyperledger.besu.ethereum.mainnet;

import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForBogota;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForByzantium;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForCancun;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForFrontier;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForFutureEIPs;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForIstanbul;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForOsaka;
import static org.hyperledger.besu.evm.precompile.MainnetPrecompiledContracts.populateForPrague;

import org.hyperledger.besu.evm.precompile.PrecompileContractRegistry;

/** Provides the various precompiled contracts used on mainnet hard forks. */
public interface MainnetPrecompiledContractRegistries {

  static PrecompileContractRegistry frontier(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForFrontier(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry byzantium(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForByzantium(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry istanbul(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForIstanbul(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry cancun(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForCancun(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry prague(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForPrague(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry osaka(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForOsaka(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry bogota(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForBogota(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }

  static PrecompileContractRegistry futureEips(
      final PrecompiledContractConfiguration precompiledContractConfiguration) {
    final PrecompileContractRegistry registry = new PrecompileContractRegistry();
    populateForFutureEIPs(registry, precompiledContractConfiguration.getGasCalculator());
    return registry;
  }
}
