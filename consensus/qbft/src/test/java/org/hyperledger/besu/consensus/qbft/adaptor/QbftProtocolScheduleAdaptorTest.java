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
package org.hyperledger.besu.consensus.qbft.adaptor;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockHeader;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockImporter;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockValidator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftProtocolSchedule;
import org.hyperledger.besu.ethereum.BlockValidator;
import org.hyperledger.besu.ethereum.ProtocolContext;
import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.core.BlockHeaderTestFixture;
import org.hyperledger.besu.ethereum.core.BlockImporter;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSchedule;
import org.hyperledger.besu.ethereum.mainnet.ProtocolSpec;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QbftProtocolScheduleAdaptorTest {
  @Mock private ProtocolSchedule besuProtocolSchedule;
  @Mock private ProtocolSpec besuProtocolSpec;
  @Mock private ProtocolContext besuProtocolContext;

  @Test
  void createsBlockValidatorUsingBesuBlockValidator() {
    final BlockHeader besuHeader = new BlockHeaderTestFixture().number(1).buildHeader();
    final QbftBlockHeader qbftHeader = new QbftBlockHeaderAdaptor(besuHeader);
    final BlockValidator besuBlockValidator = Mockito.mock(BlockValidator.class);

    when(besuProtocolSchedule.getByBlockHeader(besuHeader)).thenReturn(besuProtocolSpec);
    when(besuProtocolSpec.getBlockValidator()).thenReturn(besuBlockValidator);

    final QbftProtocolSchedule qbftProtocolSchedule =
        new QbftProtocolScheduleAdaptor(
            besuProtocolSchedule, besuProtocolContext, new ValidatedBlockCache());
    final QbftBlockValidator qbftBlockValidator =
        qbftProtocolSchedule.getBlockValidator(qbftHeader);
    assertThat(qbftBlockValidator)
        .hasFieldOrPropertyWithValue("blockValidator", besuBlockValidator);
  }

  @Test
  void createsBlockImporterUsingBesuBlockImporter() {
    final BlockHeader besuHeader = new BlockHeaderTestFixture().number(1).buildHeader();
    final QbftBlockHeader qbftHeader = new QbftBlockHeaderAdaptor(besuHeader);
    final BlockImporter besuBlockImporter = Mockito.mock(BlockImporter.class);

    when(besuProtocolSchedule.getByBlockHeader(besuHeader)).thenReturn(besuProtocolSpec);
    when(besuProtocolSpec.getBlockImporter()).thenReturn(besuBlockImporter);

    final QbftProtocolSchedule qbftProtocolSchedule =
        new QbftProtocolScheduleAdaptor(
            besuProtocolSchedule, besuProtocolContext, new ValidatedBlockCache());
    final QbftBlockImporter qbftBlockImporter = qbftProtocolSchedule.getBlockImporter(qbftHeader);
    assertThat(qbftBlockImporter).hasFieldOrPropertyWithValue("blockImporter", besuBlockImporter);
  }
}
