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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.filter;

import static org.assertj.core.api.Assertions.assertThat;
import static org.hyperledger.besu.ethereum.api.ApiConfiguration.DEFAULT_FILTER_TIMEOUT;

import java.time.Duration;
import java.time.Instant;

import org.junit.jupiter.api.Test;

public class FilterTest {

  @Test
  public void filterJustCreatedShouldNotBeExpired() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);

    assertThat(filter.isExpired()).isFalse();
  }

  @Test
  public void isExpiredShouldReturnTrueForExpiredFilter() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    filter.setExpireTime(Instant.now().minusSeconds(1));

    assertThat(filter.isExpired()).isTrue();
  }

  @Test
  public void resetExpireDateShouldIncrementExpireDate() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    filter.setExpireTime(Instant.now().minus(Duration.ofDays(1)));
    filter.resetExpireTime();

    assertThat(filter.getExpireTime())
        .isBeforeOrEqualTo(Instant.now().plus(Duration.ofMinutes(10)));
  }

  @Test
  public void configuredExpireDurationShouldBeUsedForExpireTime() {
    final Instant before = Instant.now();
    final BlockFilter filter = new BlockFilter("foo", Duration.ofMinutes(30));

    assertThat(filter.getExpireTime())
        .isBetween(before.plus(Duration.ofMinutes(30)), Instant.now().plus(Duration.ofMinutes(30)));
  }

  @Test
  public void resetExpireTimeShouldUseConfiguredDuration() {
    final BlockFilter filter = new BlockFilter("foo", Duration.ofSeconds(30));
    filter.setExpireTime(Instant.now().minus(Duration.ofDays(1)));
    filter.resetExpireTime();

    assertThat(filter.getExpireTime())
        .isBeforeOrEqualTo(Instant.now().plus(Duration.ofSeconds(30)))
        .isAfter(Instant.now());
  }
}
