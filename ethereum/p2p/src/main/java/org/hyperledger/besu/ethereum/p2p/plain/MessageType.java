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
package org.hyperledger.besu.ethereum.p2p.plain;

public enum MessageType {
  PING(0),
  PONG(1),
  DATA(2),
  UNRECOGNIZED(-1),
  ;

  private final int value;

  private MessageType(final int value) {
    this.value = value;
  }

  public int getValue() {
    return value;
  }

  public static MessageType forNumber(final int value) {
    return switch (value) {
      case 0 -> PING;
      case 1 -> PONG;
      case 2 -> DATA;
      default -> null;
    };
  }
}
