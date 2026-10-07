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

import static org.hyperledger.besu.ethereum.api.ApiConfiguration.DEFAULT_FILTER_TIMEOUT;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;

import java.util.Collections;

import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class FilterTimeoutMonitorTest {

  @Mock private FilterRepository filterRepository;

  private FilterTimeoutMonitor timeoutMonitor;

  @BeforeEach
  public void before() {
    timeoutMonitor = new FilterTimeoutMonitor(filterRepository);
  }

  @Test
  public void expiredFilterShouldBeDeleted() {
    final Filter filter = spy(new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT));
    when(filter.isExpired()).thenReturn(true);
    when(filterRepository.getFilters()).thenReturn(Lists.newArrayList(filter));

    timeoutMonitor.checkFilters();

    verify(filterRepository).getFilters();
    verify(filterRepository).delete("foo");
    verifyNoMoreInteractions(filterRepository);
  }

  @Test
  public void nonExpiredFilterShouldNotBeDeleted() {
    final Filter filter = mock(Filter.class);
    when(filter.isExpired()).thenReturn(false);
    when(filterRepository.getFilters()).thenReturn(Lists.newArrayList(filter));

    timeoutMonitor.checkFilters();

    verify(filter).isExpired();
    verifyNoMoreInteractions(filter);
  }

  @Test
  public void checkEmptyFilterRepositoryDoesNothing() {
    when(filterRepository.getFilters()).thenReturn(Collections.emptyList());

    timeoutMonitor.checkFilters();

    verify(filterRepository).getFilters();
    verifyNoMoreInteractions(filterRepository);
  }
}
