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

import org.hyperledger.besu.config.QbftConfigOptions;
import org.hyperledger.besu.consensus.common.bft.BftExtraDataCodec;
import org.hyperledger.besu.consensus.common.bft.blockcreation.BftBlockCreatorFactory;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreator;
import org.hyperledger.besu.consensus.qbft.core.types.QbftBlockCreatorFactory;

/**
 * Adaptor class to allow a {@link BftBlockCreatorFactory} to be used as a {@link
 * QbftBlockCreatorFactory}.
 */
public class QbftBlockCreatorFactoryAdaptor implements QbftBlockCreatorFactory {

  private final BftBlockCreatorFactory<QbftConfigOptions> qbftBlockCreatorFactory;
  private final BftExtraDataCodec bftExtraDataCodec;

  /**
   * Constructs a new QbftBlockCreatorFactory
   *
   * @param bftBlockCreatorFactory The Besu QBFT block creator factory
   * @param bftExtraDataCodec the bftExtraDataCodec used to encode extra data for the new header
   */
  public QbftBlockCreatorFactoryAdaptor(
      final BftBlockCreatorFactory<QbftConfigOptions> bftBlockCreatorFactory,
      final BftExtraDataCodec bftExtraDataCodec) {
    this.qbftBlockCreatorFactory = bftBlockCreatorFactory;
    this.bftExtraDataCodec = bftExtraDataCodec;
  }

  @Override
  public QbftBlockCreator create(final int roundNumber) {
    return new QbftBlockCreatorAdaptor(
        qbftBlockCreatorFactory.create(roundNumber), bftExtraDataCodec);
  }
}
