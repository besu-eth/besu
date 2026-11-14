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
package org.hyperledger.besu.consensus.clique;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.common.EpochManager;
import org.hyperledger.besu.consensus.common.validator.ValidatorProvider;
import org.hyperledger.besu.datatypes.Address;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.chain.MutableBlockchain;
import org.hyperledger.besu.ethereum.core.AddressHelpers;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;

import java.util.Collection;
import java.util.List;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

class CliqueHelpersTest {

  private final Address validator = AddressHelpers.ofValue(1);
  private final BlockHeader parent = new BlockHeaderTestFixture().buildHeader();

  private ProtocolContext contextWithTreeSetValidators() {
    final Collection<Address> validators = new TreeSet<>(List.of(validator));
    final ValidatorProvider validatorProvider = mock(ValidatorProvider.class);
    when(validatorProvider.getValidatorsAfterBlock(any())).thenReturn(validators);
    final CliqueContext cliqueContext =
        new CliqueContext(validatorProvider, new EpochManager(10), new CliqueBlockInterface());
    return new ProtocolContext.Builder()
        .withBlockchain(mock(MutableBlockchain.class))
        .withConsensusContext(cliqueContext)
        .build();
  }

  @Test
  void isSignerStillDistinguishesKnownAndUnknownValidators() {
    final ProtocolContext context = contextWithTreeSetValidators();

    assertThat(CliqueHelpers.isSigner(validator, context, parent)).isTrue();
    assertThat(CliqueHelpers.isSigner(AddressHelpers.ofValue(2), context, parent)).isFalse();
  }
}
