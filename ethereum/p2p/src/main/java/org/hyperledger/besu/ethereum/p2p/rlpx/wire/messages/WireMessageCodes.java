/*
 * Copyright ConsenSys AG.
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
package org.hyperledger.besu.ethereum.p2p.rlpx.wire.messages;

public final class WireMessageCodes {
  public static final int HELLO = 0x00;
  public static final int DISCONNECT = 0x01;
  public static final int PING = 0x02;
  public static final int PONG = 0x03;

  private WireMessageCodes() {}

  public static String messageName(final int code) {
    return switch (code) {
      case HELLO -> "Hello";
      case DISCONNECT -> "Disconnect";
      case PING -> "Ping";
      case PONG -> "Pong";
      default -> "invalid";
    };
  }
}
