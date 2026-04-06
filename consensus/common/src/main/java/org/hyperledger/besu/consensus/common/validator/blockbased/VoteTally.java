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
package org.hyperledger.besu.consensus.common.validator.blockbased;

import org.hyperledger.besu.consensus.common.validator.ValidatorVote;
import org.hyperledger.besu.datatypes.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.NavigableSet;
import java.util.Optional;
import java.util.Set;
import java.util.TreeSet;

import com.google.common.collect.Maps;

/** Tracks the current list of validators and votes to add or drop validators. */
class VoteTally {

  private final NavigableSet<Address> currentValidators;

  private final Map<Address, Set<Address>> addVotesBySubject;
  private final Map<Address, Set<Address>> removeVotesBySubject;

  VoteTally(final Collection<Address> initialValidators) {
    this(new TreeSet<>(initialValidators), new HashMap<>(), new HashMap<>());
  }

  private VoteTally(
      final Collection<Address> initialValidators,
      final Map<Address, Set<Address>> addVotesBySubject,
      final Map<Address, Set<Address>> removeVotesBySubject) {
    this.currentValidators = new TreeSet<>(initialValidators);
    this.addVotesBySubject = addVotesBySubject;
    this.removeVotesBySubject = removeVotesBySubject;
  }

  /**
   * Add a vote to the current tally. The current validator list will be updated if this vote takes
   * the tally past the required votes to approve the change.
   *
   * @param validatorVote The vote which was cast in a block header.
   */
  void addVote(final ValidatorVote validatorVote) {
    final Set<Address> addVotesForSubject =
        addVotesBySubject.computeIfAbsent(validatorVote.getRecipient(), target -> new HashSet<>());
    final Set<Address> removeVotesForSubject =
        removeVotesBySubject.computeIfAbsent(
            validatorVote.getRecipient(), target -> new HashSet<>());

    if (validatorVote.isAuthVote()) {
      addVotesForSubject.add(validatorVote.getProposer());
      removeVotesForSubject.remove(validatorVote.getProposer());
    } else {
      removeVotesForSubject.add(validatorVote.getProposer());
      addVotesForSubject.remove(validatorVote.getProposer());
    }

    final int validatorLimit = validatorLimit();
    if (addVotesForSubject.size() >= validatorLimit) {
      currentValidators.add(validatorVote.getRecipient());
      discardOutstandingVotesFor(validatorVote.getRecipient());
    }
    if (removeVotesForSubject.size() >= validatorLimit) {
      currentValidators.remove(validatorVote.getRecipient());
      discardOutstandingVotesFor(validatorVote.getRecipient());
      addVotesBySubject.values().forEach(votes -> votes.remove(validatorVote.getRecipient()));
      removeVotesBySubject.values().forEach(votes -> votes.remove(validatorVote.getRecipient()));
    }
  }

  private void discardOutstandingVotesFor(final Address subject) {
    addVotesBySubject.remove(subject);
    removeVotesBySubject.remove(subject);
  }

  Set<Address> getOutstandingAddVotesFor(final Address subject) {
    return Optional.ofNullable(addVotesBySubject.get(subject)).orElse(Collections.emptySet());
  }

  Set<Address> getOutstandingRemoveVotesFor(final Address subject) {
    return Optional.ofNullable(removeVotesBySubject.get(subject)).orElse(Collections.emptySet());
  }

  private int validatorLimit() {
    return (currentValidators.size() / 2) + 1;
  }

  /**
   * Reset the outstanding vote tallies as required at each epoch. The current validator list is
   * unaffected.
   */
  void discardOutstandingVotes() {
    addVotesBySubject.clear();
    removeVotesBySubject.clear();
  }

  /**
   * The validator addresses
   *
   * @return The collection of validators after the voting at the most recent block has been
   *     finalised.
   */
  Collection<Address> getValidators() {
    return currentValidators;
  }

  VoteTally copy() {
    final Map<Address, Set<Address>> addVotesBySubject = Maps.newHashMap();
    final Map<Address, Set<Address>> removeVotesBySubject = Maps.newHashMap();

    this.addVotesBySubject.forEach(
        (key, value) -> addVotesBySubject.put(key, new TreeSet<>(value)));
    this.removeVotesBySubject.forEach(
        (key, value) -> removeVotesBySubject.put(key, new TreeSet<>(value)));

    return new VoteTally(
        new TreeSet<>(this.currentValidators), addVotesBySubject, removeVotesBySubject);
  }
}
