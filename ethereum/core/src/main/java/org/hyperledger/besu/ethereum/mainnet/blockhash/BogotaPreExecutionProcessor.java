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
package org.hyperledger.besu.ethereum.mainnet.blockhash;

import org.hyperledger.besu.ethereum.mainnet.BogotaForkInstall;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.account.Account;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.Optional;

/**
 * Installs the EIP-8141 expiry verifier's runtime code, on top of the pre-execution work Bogota
 * inherits from Prague.
 *
 * <p>EIP-8141 installs the code when the fork activates. The install is expressed here as "install
 * it when it is not there", which is the same thing: nothing but the fork can put code at that
 * address, so the condition is true exactly once, at the first block of the fork. Stating it that
 * way means no comparison against the parent block, which keeps it correct across reorgs and for a
 * chain whose genesis already carries the code.
 *
 * <p>Only the code is written. The account's other fields are left as they were, so an address
 * nobody had touched keeps a zero nonce and balance, and an account that already existed keeps its
 * nonce, balance and storage.
 *
 * <p>The install is not a system call, so unlike its siblings here it is applied without the
 * EIP-7928 access location tracker: the fork transition is not a block-level operation and must
 * leave no trace in the block access list, in the activation block included.
 */
public class BogotaPreExecutionProcessor extends PraguePreExecutionProcessor {

  @Override
  public Void process(
      final BlockProcessingContext context,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    super.process(context, accessLocationTracker);

    // The same updater shape the system calls above use: the install is made in a child of the
    // block updater and committed through it, rather than applied to the block accumulator
    // directly. Committing the accumulator itself does not reach the diff the world state
    // persists, which costs the activation block its state root.
    final WorldUpdater blockUpdater = context.getWorldState().updater();
    final Account verifier = blockUpdater.get(BogotaForkInstall.EXPIRY_VERIFIER);
    if (verifier == null || verifier.getCode().isEmpty()) {
      final WorldUpdater installUpdater = blockUpdater.updater();
      BogotaForkInstall.installExpiryVerifier(installUpdater);
      installUpdater.commit();
      blockUpdater.commit();
    }
    return null;
  }
}
