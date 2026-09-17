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
 * Ensures that a block header carries the {@code requestsHash} field once EIP-7685 is active. From
 * Prague onwards every block header must include {@code requestsHash}, including blocks whose
 * execution requests list is empty (in which case the field holds the empty requests hash). A
 * header that omits the field entirely is invalid regardless of its block body, so this presence
 * check is intentionally independent of the parent header.
 */
public class RequestsHashPresentValidationRule implements DetachedBlockHeaderValidationRule {

  private static final Logger LOG =
      LoggerFactory.getLogger(RequestsHashPresentValidationRule.class);

  @Override
  public boolean validate(final BlockHeader header, final BlockHeader parent) {
    if (header.getRequestsHash().isEmpty()) {
      LOG.info(
          "Invalid block header: requestsHash field is required from Prague onwards but is missing");
      return false;
    }
    return true;
  }

  @Override
  public String toString() {
    return "RequestsHashPresent";
  }
}
