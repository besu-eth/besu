/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.ethereum.chain;

import static org.hyperledger.besu.plugin.data.BadBlockCause.BadBlockReason.DESCENDS_FROM_BAD_BLOCK;
import static org.hyperledger.besu.plugin.data.BadBlockCause.BadBlockReason.SPEC_VALIDATION_FAILURE;

import org.hyperledger.besu.ethereum.core.Block;
import org.hyperledger.besu.ethereum.core.BlockHeader;

import com.google.common.base.MoreObjects;

public class BadBlockCause implements org.hyperledger.besu.plugin.data.BadBlockCause {

  private final BadBlockReason reason;
  private final String description;

  public static BadBlockCause fromBadAncestorBlock(final Block badAncestor) {
    return fromBadAncestorHeader(badAncestor.getHeader());
  }

  public static BadBlockCause fromBadAncestorHeader(final BlockHeader badAncestor) {
    final String description =
        String.format("Descends from bad block %s", badAncestor.toLogString());
    return new BadBlockCause(DESCENDS_FROM_BAD_BLOCK, description);
  }

  public static BadBlockCause fromValidationFailure(final String failureMessage) {
    return new BadBlockCause(SPEC_VALIDATION_FAILURE, failureMessage);
  }

  private BadBlockCause(final BadBlockReason reason, final String description) {
    this.reason = reason;
    this.description = description;
  }

  @Override
  public BadBlockReason getReason() {
    return reason;
  }

  @Override
  public String getDescription() {
    return description;
  }

  @Override
  public String toString() {
    return MoreObjects.toStringHelper(this)
        .add("reason", reason)
        .add("description", description)
        .toString();
  }
}
