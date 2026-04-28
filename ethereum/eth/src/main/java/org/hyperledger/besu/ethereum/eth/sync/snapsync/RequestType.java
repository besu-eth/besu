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
package org.hyperledger.besu.ethereum.eth.sync.snapsync;

public enum RequestType {
  ACCOUNT_RANGE((byte) 1),
  STORAGE_RANGE((byte) 2),
  BYTECODES((byte) 3),
  TRIE_NODE((byte) 4);

  private final byte value;

  RequestType(final byte value) {
    this.value = value;
  }

  public byte getValue() {
    return value;
  }

  public static RequestType fromValue(final byte value) {
    switch (value) {
      case (byte) 1:
        return ACCOUNT_RANGE;
      case (byte) 2:
        return STORAGE_RANGE;
      case (byte) 3:
        return BYTECODES;
      case (byte) 4:
        return TRIE_NODE;
      default:
        throw new IllegalArgumentException("Invalid value supplied");
    }
  }
}
