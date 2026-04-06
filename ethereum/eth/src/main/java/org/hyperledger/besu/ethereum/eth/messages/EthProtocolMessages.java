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
package org.hyperledger.besu.ethereum.eth.messages;

public final class EthProtocolMessages {

  public static final int STATUS = 0x00;

  public static final int NEW_BLOCK_HASHES = 0x01;

  public static final int TRANSACTIONS = 0x02;

  public static final int GET_BLOCK_HEADERS = 0x03;

  public static final int BLOCK_HEADERS = 0x04;

  public static final int GET_BLOCK_BODIES = 0x05;

  public static final int BLOCK_BODIES = 0x06;

  public static final int NEW_BLOCK = 0X07;

  // Eth63 messages
  public static final int GET_NODE_DATA = 0x0D;

  public static final int NODE_DATA = 0x0E;

  public static final int GET_RECEIPTS = 0x0F;

  public static final int RECEIPTS = 0x10;

  // Eth65 messages
  public static final int NEW_POOLED_TRANSACTION_HASHES = 0x08;

  public static final int GET_POOLED_TRANSACTIONS = 0x09;

  public static final int POOLED_TRANSACTIONS = 0x0A;

  // Eth69 messages
  public static final int BLOCK_RANGE_UPDATE = 0x11;

  // Eth71 messages
  public static final int GET_BLOCK_ACCESS_LISTS = 0x12;

  public static final int BLOCK_ACCESS_LISTS = 0x13;

  private EthProtocolMessages() {
    // Holder for constants only
  }
}
