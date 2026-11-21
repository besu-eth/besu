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
package org.hyperledger.besu.evm.precompile;

import static org.hyperledger.besu.datatypes.Address.P256_VERIFY;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.evm.gascalculator.GasCalculator;

/** Provides the various precompiled contracts used on mainnet hard forks. */
public interface MainnetPrecompiledContracts {

  /**
   * Frontier precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry frontier(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForFrontier(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for frontier.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForFrontier(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    registry.put(Address.ECREC, new ECRECPrecompiledContract(gasCalculator));
    registry.put(Address.SHA256, new SHA256PrecompiledContract(gasCalculator));
    registry.put(Address.RIPEMD160, new RIPEMD160PrecompiledContract(gasCalculator));
    registry.put(Address.ID, new IDPrecompiledContract(gasCalculator));
  }

  /**
   * Homestead precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry homestead(final GasCalculator gasCalculator) {
    return frontier(gasCalculator);
  }

  /**
   * Byzantium precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry byzantium(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForByzantium(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for byzantium.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForByzantium(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForFrontier(registry, gasCalculator);
    registry.put(
        Address.MODEXP,
        new BigIntegerModularExponentiationPrecompiledContract(gasCalculator, Long.MAX_VALUE));
    registry.put(Address.ALTBN128_ADD, new AltBN128AddPrecompiledContract(gasCalculator, 500L));
    registry.put(Address.ALTBN128_MUL, new AltBN128MulPrecompiledContract(gasCalculator, 40_000L));
    registry.put(
        Address.ALTBN128_PAIRING,
        new AltBN128PairingPrecompiledContract(gasCalculator, 80_000L, 100_000L));
  }

  /**
   * Istanbul precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry istanbul(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForIstanbul(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for istanbul.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForIstanbul(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForByzantium(registry, gasCalculator);
    registry.put(Address.ALTBN128_ADD, new AltBN128AddPrecompiledContract(gasCalculator, 150L));
    registry.put(Address.ALTBN128_MUL, new AltBN128MulPrecompiledContract(gasCalculator, 6_000L));
    registry.put(
        Address.ALTBN128_PAIRING,
        new AltBN128PairingPrecompiledContract(gasCalculator, 34_000L, 45_000L));
    registry.put(Address.BLAKE2B_F_COMPRESSION, new BLAKE2BFPrecompileContract(gasCalculator));
  }

  /**
   * Cancun precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry cancun(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForCancun(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for Cancun.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForCancun(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForIstanbul(registry, gasCalculator);

    // EIP-4844 - shard blob transactions
    registry.put(Address.KZG_POINT_EVAL, new KZGPointEvalPrecompiledContract());
  }

  /**
   * Prague precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry prague(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForPrague(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for Prague.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForPrague(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForCancun(registry, gasCalculator);

    // EIP-2537 - BLS12-381 curve operations
    registry.put(Address.BLS12_G1ADD, new BLS12G1AddPrecompiledContract());
    registry.put(Address.BLS12_G1MULTIEXP, new BLS12G1MultiExpPrecompiledContract());
    registry.put(Address.BLS12_G2ADD, new BLS12G2AddPrecompiledContract());
    registry.put(Address.BLS12_G2MULTIEXP, new BLS12G2MultiExpPrecompiledContract());
    registry.put(Address.BLS12_PAIRING, new BLS12PairingPrecompiledContract());
    registry.put(Address.BLS12_MAP_FP_TO_G1, new BLS12MapFpToG1PrecompiledContract());
    registry.put(Address.BLS12_MAP_FP2_TO_G2, new BLS12MapFp2ToG2PrecompiledContract());
  }

  /**
   * Osaka precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry osaka(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForOsaka(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for Osaka.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForOsaka(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForPrague(registry, gasCalculator);

    // EIP-7823 - Set upper bounds for MODEXP
    registry.put(
        Address.MODEXP,
        new BigIntegerModularExponentiationPrecompiledContract(gasCalculator, 1024L));
    // EIP-7951 - secp256r1 P256VERIFY
    registry.put(P256_VERIFY, new P256VerifyPrecompiledContract(gasCalculator));
  }

  /**
   * Populate registry for Bogota.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForBogota(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForOsaka(registry, gasCalculator);

    // EIP-8151 - account code restricted ecrecover
    registry.put(Address.ECREC, new AccountCodeRestrictedECRECPrecompiledContract(gasCalculator));
  }

  /**
   * FutureEIPs precompile contract registry.
   *
   * @param gasCalculator the gas calculator
   * @return the precompile contract registry
   */
  static PrecompileContractRegistry futureEIPs(final GasCalculator gasCalculator) {
    PrecompileContractRegistry precompileContractRegistry = new PrecompileContractRegistry();
    populateForFutureEIPs(precompileContractRegistry, gasCalculator);
    return precompileContractRegistry;
  }

  /**
   * Populate registry for Future EIPs.
   *
   * @param registry the registry
   * @param gasCalculator the gas calculator
   */
  static void populateForFutureEIPs(
      final PrecompileContractRegistry registry, final GasCalculator gasCalculator) {
    populateForCancun(registry, gasCalculator);
  }
}
