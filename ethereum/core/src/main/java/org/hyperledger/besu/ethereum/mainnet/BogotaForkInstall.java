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
package org.hyperledger.besu.ethereum.mainnet;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.evm.account.MutableAccount;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import org.apache.tuweni.bytes.Bytes;

/**
 * EIP-8141 installs the expiry verifier contract's runtime code when the Bogota fork activates.
 *
 * <p>Only the code is installed. The account's other fields are left as they were, so an address
 * nobody had touched keeps a zero nonce and balance, and an account that already existed keeps its
 * nonce, balance and storage.
 *
 * <p>The install is not a transaction and not a system call: it belongs to the fork transition
 * itself. It therefore leaves no trace in the block's EIP-7928 access list, which is why it runs
 * before the block processor builds one rather than through a system call.
 */
public final class BogotaForkInstall {

  /** The EIP-8141 expiry verifier contract address. */
  public static final Address EXPIRY_VERIFIER = MainnetTransactionValidator.EXPIRY_VERIFIER;

  /**
   * The expiry verifier's runtime code: reject a call whose data is not exactly 8 bytes, then treat
   * those bytes as a big-endian expiry timestamp and halt successfully only while the block
   * timestamp is below it.
   */
  public static final Bytes EXPIRY_VERIFIER_CODE =
      Bytes.fromHexString("0x60083614600a575f5ffd5b5f3560c01c4211601657005b5f5ffd");

  private BogotaForkInstall() {}

  /**
   * Whether {@code blockHeader} is the first block of the Bogota fork, and so the one that performs
   * the install.
   *
   * <p>A fork active from genesis has no transition block: there is no earlier state for the code
   * to be absent from, so the genesis allocation carries it instead.
   *
   * @param parentHeader the header of the block being processed
   * @param blockHeader the header of the block being processed
   * @param bogotaTime the configured Bogota activation timestamp
   * @return true when the fork activates at this block
   */
  public static boolean isActivationBlock(
      final BlockHeader parentHeader, final BlockHeader blockHeader, final long bogotaTime) {
    return blockHeader.getTimestamp() >= bogotaTime && parentHeader.getTimestamp() < bogotaTime;
  }

  /**
   * Installs the expiry verifier's runtime code, preserving every other field of the account.
   *
   * @param worldUpdater the updater to apply the install to
   */
  public static void installExpiryVerifier(final WorldUpdater worldUpdater) {
    final MutableAccount verifier = worldUpdater.getOrCreate(EXPIRY_VERIFIER);
    verifier.setCode(EXPIRY_VERIFIER_CODE);
  }
}
