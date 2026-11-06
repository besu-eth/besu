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
package org.hyperledger.besu.ethereum.mainnet.blockhash;

import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.chain.Blockchain;
import org.hyperledger.besu.ethereum.core.ProcessableBlockHeader;
import org.hyperledger.besu.ethereum.mainnet.block.access.list.AccessLocationTracker;
import org.hyperledger.besu.ethereum.mainnet.systemcall.BlockProcessingContext;
import org.hyperledger.besu.evm.blockhash.BlockHashLookup;
import org.hyperledger.besu.evm.worldstate.WorldUpdater;

import java.util.List;
import java.util.Optional;

/**
 * Sets the nonce of every listed zero-nonce storage account to one at the start of the fork block,
 * then runs the wrapped pre-execution processor (EIP-8253).
 *
 * <p>The bump runs before the pre-execution system calls and is recorded at block access index
 * zero. As in the reference implementation, it applies unconditionally: a missing account is
 * created with nonce one, and an account whose nonce is already above one is set back to one.
 */
public class ZeroNonceStorageAccountsPreExecutionProcessor implements PreExecutionProcessor {

  private final PreExecutionProcessor delegate;
  private final long forkTimestamp;
  private final List<Address> zeroNonceStorageAccounts;

  /**
   * Wraps a pre-execution processor with the EIP-8253 fork-block bump.
   *
   * @param delegate the pre-execution processor to run after the bump
   * @param forkTimestamp the activation timestamp of the fork that performs the bump
   * @param zeroNonceStorageAccounts the accounts to bump, from the chain configuration
   */
  public ZeroNonceStorageAccountsPreExecutionProcessor(
      final PreExecutionProcessor delegate,
      final long forkTimestamp,
      final List<Address> zeroNonceStorageAccounts) {
    this.delegate = delegate;
    this.forkTimestamp = forkTimestamp;
    this.zeroNonceStorageAccounts = List.copyOf(zeroNonceStorageAccounts);
  }

  @Override
  public Void process(
      final BlockProcessingContext context,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    if (isForkBlock(context)) {
      bumpNonces(context, accessLocationTracker);
    }
    return delegate.process(context, accessLocationTracker);
  }

  private boolean isForkBlock(final BlockProcessingContext context) {
    return context.getBlockHeader().getTimestamp() >= forkTimestamp
        && context.getParentTimestamp().map(parent -> parent < forkTimestamp).orElse(false);
  }

  private void bumpNonces(
      final BlockProcessingContext context,
      final Optional<AccessLocationTracker> accessLocationTracker) {
    final WorldUpdater blockUpdater = context.getWorldState().updater();
    // The accounts blockUpdater wraps keep the pre-bump state the access list diffs against.
    final WorldUpdater bumpUpdater = blockUpdater.updater();
    for (final Address address : zeroNonceStorageAccounts) {
      accessLocationTracker.ifPresent(tracker -> tracker.addTouchedAccount(address));
      bumpUpdater.getOrCreate(address).setNonce(1L);
    }
    accessLocationTracker.ifPresent(
        tracker ->
            context
                .getBlockAccessListBuilder()
                .ifPresent(builder -> builder.apply(tracker, bumpUpdater)));
    bumpUpdater.commit();
    blockUpdater.commit();
  }

  @Override
  public BlockHashLookup createBlockHashLookup(
      final Blockchain blockchain, final ProcessableBlockHeader blockHeader) {
    return delegate.createBlockHashLookup(blockchain, blockHeader);
  }

  @Override
  public Optional<Address> getHistoryContract() {
    return delegate.getHistoryContract();
  }

  @Override
  public Optional<Address> getBeaconRootsContract() {
    return delegate.getBeaconRootsContract();
  }
}
