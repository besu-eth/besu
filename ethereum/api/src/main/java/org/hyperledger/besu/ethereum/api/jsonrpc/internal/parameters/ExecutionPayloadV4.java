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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.parameters;

import org.hyperledger.besu.datatypes.parameters.UnsignedLongParameter;

import com.fasterxml.jackson.annotation.JsonSetter;

public final class ExecutionPayloadV4 extends ExecutionPayloadV3 {
  private String blockAccessList;
  private Long slotNumber;

  @JsonSetter("blockAccessList")
  public void setBlockAccessList(final String blockAccessList) {
    this.blockAccessList = blockAccessList;
  }

  @JsonSetter("slotNumber")
  public void setSlotNumber(final UnsignedLongParameter slotNumber) {
    this.slotNumber = slotNumber == null ? null : slotNumber.getValue();
  }

  public String getBlockAccessList() {
    return blockAccessList;
  }

  public Long getSlotNumber() {
    return slotNumber;
  }
}
