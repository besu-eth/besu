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
package org.hyperledger.besu.ethereum.mainnet.headervalidationrules;

import org.hyperledger.besu.ethereum.core.BlockHeader;
import org.hyperledger.besu.ethereum.mainnet.DetachedBlockHeaderValidationRule;

import org.apache.tuweni.bytes.Bytes;

/**
 * The DAO fork's header rule (EIP-779): the DAO fork block and the nine blocks after it must carry
 * {@code "dao-hard-fork"} as extra data. Other blocks are not checked.
 */
public class DaoExtraDataValidationRule implements DetachedBlockHeaderValidationRule {

  /** {@code "dao-hard-fork"}. */
  public static final Bytes DAO_EXTRA_DATA = Bytes.fromHexString("0x64616f2d686172642d666f726b");

  /** The number of blocks, from the DAO fork block, that must carry the DAO extra data. */
  static final long DAO_EXTRA_DATA_RANGE = 10;

  private static final ConstantFieldValidationRule<Bytes> EXTRA_DATA_RULE =
      new ConstantFieldValidationRule<>("extraData", BlockHeader::getExtraData, DAO_EXTRA_DATA);

  private final long daoForkBlock;

  /**
   * @param daoForkBlock the number of the DAO fork block
   */
  public DaoExtraDataValidationRule(final long daoForkBlock) {
    this.daoForkBlock = daoForkBlock;
  }

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    final long number = header.getNumber();
    if (number < daoForkBlock || number >= daoForkBlock + DAO_EXTRA_DATA_RANGE) {
      return true;
    }
    return EXTRA_DATA_RULE.validate(header, parent);
  }
}
