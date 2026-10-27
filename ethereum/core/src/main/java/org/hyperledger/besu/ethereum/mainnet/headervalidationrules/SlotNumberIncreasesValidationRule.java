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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * EIP-8198: the slot number of a block must be greater than the parent one, so that the spacing in
 * slots between them, from which the base fee is derived, is always positive.
 */
public class SlotNumberIncreasesValidationRule implements DetachedBlockHeaderValidationRule {

  private static final Logger LOG =
      LoggerFactory.getLogger(SlotNumberIncreasesValidationRule.class);

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    // the slot number is an unsigned 64-bit value
    if (Long.compareUnsigned(header.getSlotNumber(), parent.getSlotNumber()) <= 0) {
      LOG.info(
          "Invalid block header: slot number {} is not greater than the parent one {}",
          Long.toUnsignedString(header.getSlotNumber()),
          Long.toUnsignedString(parent.getSlotNumber()));
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "SlotNumberIncreases";
  }
}
