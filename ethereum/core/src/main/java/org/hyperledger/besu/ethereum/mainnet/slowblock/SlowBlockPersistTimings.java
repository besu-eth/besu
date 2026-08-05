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
package org.hyperledger.besu.ethereum.mainnet.slowblock;

/**
 * The only slow-block surface the world state persist path knows about: two setters it calls while
 * committing a block.
 *
 * <p>Kept deliberately narrow so the in-flight state-root refactor has a single, small contact
 * point to reland against rather than a dependency on the whole metrics aggregate.
 */
public interface SlowBlockPersistTimings {

  /**
   * Records how long the import thread waited for the block's state root.
   *
   * @param nanos elapsed nanoseconds
   */
  void setStateHashWaitNanos(long nanos);

  /**
   * Records how long committing the world state to storage took.
   *
   * @param nanos elapsed nanoseconds
   */
  void setCommitNanos(long nanos);
}
