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
package org.hyperledger.besu.ethereum.eth.messages.snap;

import java.util.Set;

public final class SnapV2 {

  public static final int GET_ACCOUNT_RANGE = 0x00;
  public static final int ACCOUNT_RANGE = 0x01;
  public static final int GET_STORAGE_RANGE = 0x02;
  public static final int STORAGE_RANGE = 0x03;
  public static final int GET_BYTECODES = 0x04;
  public static final int BYTECODES = 0x05;
  public static final int GET_BLOCK_ACCESS_LISTS = 0x08;
  public static final int BLOCK_ACCESS_LISTS = 0x09;

  /** The set of snap/2-only inbound request message codes that the snap server must handle. */
  public static final Set<Integer> REQUEST_CODES = Set.of(GET_BLOCK_ACCESS_LISTS);

  private SnapV2() {
    // Holder for constants only
  }
}
