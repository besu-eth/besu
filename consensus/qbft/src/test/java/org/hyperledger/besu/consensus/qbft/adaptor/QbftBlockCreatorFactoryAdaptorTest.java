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

import org.hyperledger.besu.consensus.qbft.QbftExtraDataCodec;
import org.hyperledger.besu.consensus.qbft.blockcreation.QbftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreator;
import org.hyperledger.besu.ethereum.blockcreation.BlockCreator;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class QbftBlockCreatorFactoryAdaptorTest {
  @Mock private QbftBlockCreatorFactory qbftBlockCreatorFactory;
  @Mock private BlockCreator blockCreator;

  @Test
  void createsQbftBlockCreatorFactory() {
    when(qbftBlockCreatorFactory.create(1)).thenReturn(blockCreator);

    QbftBlockCreatorFactoryAdaptor qbftBlockCreatorFactoryAdaptor =
        new QbftBlockCreatorFactoryAdaptor(
            qbftBlockCreatorFactory, new QbftExtraDataCodec(), new ValidatedBlockCache());
    QbftBlockCreator qbftBlockCreator = qbftBlockCreatorFactoryAdaptor.create(1);
    assertThat(qbftBlockCreator).hasFieldOrPropertyWithValue("besuBlockCreator", blockCreator);
  }
}
