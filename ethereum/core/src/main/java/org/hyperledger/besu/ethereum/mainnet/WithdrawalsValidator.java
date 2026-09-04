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

import static com.google.common.base.Preconditions.checkArgument;

import org.hyperledger.besu.datatypes.Hash;
import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.Withdrawal;

import java.util.List;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public interface WithdrawalsValidator {

  boolean validateWithdrawals(Optional<List<Withdrawal>> withdrawals);

  boolean validateWithdrawalsRoot(Block block);

  class ProhibitedWithdrawals implements WithdrawalsValidator {

    private static final Logger LOG = LoggerFactory.getLogger(ProhibitedWithdrawals.class);

    @Override
    public boolean validateWithdrawals(final Optional<List<Withdrawal>> withdrawals) {
      final boolean isValid = withdrawals.isEmpty();
      if (!isValid) {
        LOG.warn(
            "withdrawals must be null when Withdrawals are prohibited but were: {}", withdrawals);
      }
      return isValid;
    }

    @Override
    public boolean validateWithdrawalsRoot(final Block block) {
      final Optional<Hash> withdrawalsRoot = block.getHeader().getWithdrawalsRoot();
      if (withdrawalsRoot.isPresent()) {
        LOG.warn(
            "withdrawalsRoot must be null when Withdrawals are prohibited but was: {}",
            withdrawalsRoot.get());
        return false;
      }

      return true;
    }
  }

  class AllowedWithdrawals implements WithdrawalsValidator {

    private static final Logger LOG = LoggerFactory.getLogger(AllowedWithdrawals.class);

    @Override
    public boolean validateWithdrawals(final Optional<List<Withdrawal>> withdrawals) {
      final boolean isValid = withdrawals.isPresent();
      if (!isValid) {
        LOG.warn("withdrawals must not be null when Withdrawals are activated");
      }
      return isValid;
    }

    @Override
    public boolean validateWithdrawalsRoot(final Block block) {
      checkArgument(
          block.getBody().getWithdrawals().isPresent(), "Block body must contain withdrawals");
      final Optional<Hash> withdrawalsRoot = block.getHeader().getWithdrawalsRoot();
      if (withdrawalsRoot.isEmpty()) {
        LOG.warn("withdrawalsRoot must not be null when Withdrawals are activated");
        return false;
      }

      final List<Withdrawal> withdrawals = block.getBody().getWithdrawals().get();
      final Hash expectedWithdrawalsRoot = BodyValidation.withdrawalsRoot(withdrawals);
      if (!expectedWithdrawalsRoot.equals(withdrawalsRoot.get())) {
        LOG.info(
            "Invalid block: transaction root mismatch (expected={}, actual={})",
            expectedWithdrawalsRoot,
            withdrawalsRoot.get());
        return false;
      }

      return true;
    }
  }

  class NotApplicableWithdrawals implements WithdrawalsValidator {

    private static final Logger LOG = LoggerFactory.getLogger(NotApplicableWithdrawals.class);

    @Override
    public boolean validateWithdrawals(final Optional<List<Withdrawal>> withdrawals) {
      // Pre-Shanghai blocks have no withdrawals field (Optional.empty()) — valid.
      // Shanghai+ blocks must carry an empty list; a non-empty list means a Byzantine
      // proposer injected unauthorized withdrawals that would be executed as balance credits.
      if (withdrawals.isPresent() && !withdrawals.get().isEmpty()) {
        LOG.warn(
            "Withdrawals not applicable but block contained {} withdrawal(s)",
            withdrawals.get().size());
        return false;
      }
      return true;
    }

    @Override
    public boolean validateWithdrawalsRoot(final Block block) {
      final Optional<Hash> withdrawalsRoot = block.getHeader().getWithdrawalsRoot();
      if (withdrawalsRoot.isEmpty()) {
        return true; // pre-Shanghai block, no withdrawals root field
      }
      // A block with a withdrawalsRoot in the header but no withdrawals field in the body is
      // malformed.
      if (block.getBody().getWithdrawals().isEmpty()) {
        LOG.warn(
            "Invalid block: withdrawalsRoot present in header but withdrawals absent from body");
        return false;
      }
      if (!Hash.EMPTY_TRIE_HASH.equals(withdrawalsRoot.get())) {
        LOG.warn(
            "Invalid block: withdrawals root mismatch (expected={}, actual={})",
            Hash.EMPTY_TRIE_HASH,
            withdrawalsRoot.get());
        return false;
      }
      return true;
    }
  }
}
