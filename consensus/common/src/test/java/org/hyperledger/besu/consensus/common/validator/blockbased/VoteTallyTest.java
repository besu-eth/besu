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

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.assertj.core.api.Assertions.assertThat;

import org.hyperledger.besu.consensus.common.validator.ValidatorVote;
import org.hyperledger.besu.consensus.common.validator.VoteType;
import org.hyperledger.besu.datatypes.Address;

import org.junit.jupiter.api.Test;

public class VoteTallyTest {

  private static final Address validator1 =
      Address.fromHexString("000d836201318ec6899a67540690382780743280");
  private static final Address validator2 =
      Address.fromHexString("001762430ea9c3a26e5749afdb70da5f78ddbb8c");
  private static final Address validator3 =
      Address.fromHexString("001d14804b399c6ef80e64576f657660804fec0b");
  private static final Address validator4 =
      Address.fromHexString("0032403587947b9f15622a68d104d54d33dbd1cd");
  private static final Address validator5 =
      Address.fromHexString("00497e92cdc0e0b963d752b2296acb87da828b24");

  @Test
  public void validatorsAreNotAddedBeforeRequiredVoteCountReached() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void validatorAddedToListWhenMoreThanHalfOfProposersVoteToAdd() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);
  }

  @Test
  public void validatorsAreAddedInCorrectOrder() {
    final VoteTally voteTally =
        new VoteTally(asList(validator1, validator2, validator3, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator4));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);
  }

  @Test
  public void duplicateVotesFromSameProposerAreIgnored() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void proposerChangingAddVoteToDropBeforeLimitReachedDiscardsAddVote() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void proposerChangingAddVoteToDropAfterLimitReachedPreservesAddVote() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);
  }

  @Test
  public void clearVotesAboutAValidatorWhenItIsAdded() {
    final VoteTally voteTally = fourValidators();
    // Vote to add validator5
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);

    // Then vote it back out
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator4, validator5));
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);

    // And then start voting to add it back in, but validator1's vote should have been discarded
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void requiresASingleVoteWhenThereIsOnlyOneValidator() {
    final VoteTally voteTally = new VoteTally(singletonList(validator1));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator2));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2);
  }

  @Test
  public void requiresTwoVotesWhenThereAreTwoValidators() {
    final VoteTally voteTally = new VoteTally(asList(validator1, validator2));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator3));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2);

    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator3));
    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator3);
  }

  @Test
  public void resetVotes() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.discardOutstandingVotes();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void resetDropVotes() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.discardOutstandingVotes();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator4));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void validatorsAreNotRemovedBeforeRequiredVoteCountReached() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void validatorRemovedFromListWhenMoreThanHalfOfProposersVoteToDrop() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator4));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator3);
  }

  @Test
  public void validatorsAreInCorrectOrderAfterRemoval() {
    final VoteTally voteTally = new VoteTally(asList(validator1, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator3));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator3));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator4, validator3));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator4);
  }

  @Test
  public void duplicateDropVotesFromSameProposerAreIgnored() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void proposerChangingDropVoteToAddBeforeLimitReachedDiscardsDropVote() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator4));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);
  }

  @Test
  public void proposerChangingDropVoteToAddAfterLimitReachedPreservesDropVote() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator4));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator3);
  }

  @Test
  public void removedValidatorsVotesAreDiscarded() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator4, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator4, validator3));

    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator4));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator4));
    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator3);

    // Now adding only requires 2 votes (>50% of the 3 remaining validators)
    // but validator4's vote no longer counts
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator3));

    assertThat(voteTally.getValidators()).containsExactly(validator1, validator2, validator3);
  }

  @Test
  public void clearVotesAboutAValidatorWhenItIsDropped() {
    final VoteTally voteTally =
        new VoteTally(asList(validator1, validator2, validator3, validator4, validator5));
    // Vote to remove validator5
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator5));

    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);

    // Then vote it back in
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator4, validator5));
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);

    // And then start voting to drop it again, but validator1's vote should have been discarded
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator5));
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);
  }

  @Test
  public void trackMultipleOngoingVotesIndependently() {
    final VoteTally voteTally = fourValidators();
    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator1, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator1, validator3));

    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator2, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator2, validator1));

    // Neither vote has enough votes to complete.
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4);

    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator3, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator3, validator1));

    // Validator 5 now has 3 votes and is added
    assertThat(voteTally.getValidators())
        .containsExactly(validator1, validator2, validator3, validator4, validator5);

    voteTally.addVote(new ValidatorVote(VoteType.ADD, validator4, validator5));
    voteTally.addVote(new ValidatorVote(VoteType.DROP, validator4, validator1));

    // Validator 1 now gets dropped.
    assertThat(voteTally.getValidators())
        .containsExactly(validator2, validator3, validator4, validator5);
  }

  private VoteTally fourValidators() {
    return new VoteTally(asList(validator1, validator2, validator3, validator4));
  }
}
