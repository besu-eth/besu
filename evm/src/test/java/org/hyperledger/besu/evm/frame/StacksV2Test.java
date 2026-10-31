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
package org.hyperledger.besu.evm.frame;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class StacksV2Test {

  private static final int LENGTH = 1024 * 4;

  @Test
  void nextFrameAtTheSameDepthReusesTheReleasedStack() {
    final long[] first = StacksV2.take(3, LENGTH);
    StacksV2.release(3, first);

    assertThat(StacksV2.take(3, LENGTH)).isSameAs(first);
  }

  @Test
  void releasedStackIsTakenOnlyOnce() {
    final long[] released = StacksV2.take(3, LENGTH);
    StacksV2.release(3, released);

    final long[] first = StacksV2.take(3, LENGTH);
    final long[] second = StacksV2.take(3, LENGTH);

    assertThat(first).isSameAs(released);
    assertThat(second).isNotSameAs(released);
  }

  @Test
  void frameAtAnotherDepthGetsItsOwnStack() {
    final long[] parent = StacksV2.take(3, LENGTH);
    StacksV2.release(3, parent);

    assertThat(StacksV2.take(4, LENGTH)).isNotSameAs(parent);
  }

  @Test
  void otherLengthGetsANewStack() {
    final long[] first = StacksV2.take(3, LENGTH);
    StacksV2.release(3, first);

    assertThat(StacksV2.take(3, 16 * 4)).isNotSameAs(first).hasSize(16 * 4);
  }

  @Test
  void framesBelowTheCachedDepthsAllocate() {
    final long[] first = StacksV2.take(StacksV2.CACHED_DEPTHS, LENGTH);
    StacksV2.release(StacksV2.CACHED_DEPTHS, first);

    assertThat(StacksV2.take(StacksV2.CACHED_DEPTHS, LENGTH)).isNotSameAs(first);
  }
}
