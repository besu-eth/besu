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
 * Ensures that a block header carries the {@code slotNumber} field once EIP-7843 is active. The
 * field is the last one in the header RLP, so a header that omits it still decodes cleanly - only
 * this presence check distinguishes it from a well-formed one. As with {@code requestsHash}, a
 * header missing the field is invalid regardless of its block body, so the check is independent of
 * the parent header.
 */
public class SlotNumberPresentValidationRule implements DetachedBlockHeaderValidationRule {

  private static final Logger LOG = LoggerFactory.getLogger(SlotNumberPresentValidationRule.class);

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    if (header.getOptionalSlotNumber().isEmpty()) {
      LOG.info(
          "Invalid block header: slotNumber field is required from Amsterdam onwards but is missing");
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "SlotNumberPresent";
  }
}
