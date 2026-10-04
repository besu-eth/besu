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
package org.hyperledger.besu.ethereum.mainnet.systemcall;

/**
 * A system call ran but did not complete successfully (revert or exceptional halt). Its writes are
 * discarded. Request contract calls (EIP-7002, EIP-7251) must reject the block on this, while the
 * EIP-4788 and EIP-2935 calls ignore it.
 */
public class SystemCallFailedException extends RuntimeException {
  public SystemCallFailedException(final String message) {
    super(message);
  }
}
