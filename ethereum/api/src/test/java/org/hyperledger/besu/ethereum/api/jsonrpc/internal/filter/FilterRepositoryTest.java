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
import static org.assertj.core.api.Assertions.catchThrowable;
import static org.hyperledger.besu.ethereum.api.ApiConfiguration.DEFAULT_FILTER_TIMEOUT;
import static org.hyperledger.besu.ethereum.api.ApiConfiguration.DEFAULT_MAX_FILTER_COUNT;

import java.util.Collection;
import java.util.Optional;

import org.assertj.core.util.Lists;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

public class FilterRepositoryTest {

  private FilterRepository repository;

  @BeforeEach
  public void before() {
    repository = new FilterRepository(DEFAULT_MAX_FILTER_COUNT);
  }

  @Test
  public void getFiltersShouldReturnAllFilters() {
    final BlockFilter filter1 = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    final BlockFilter filter2 = new BlockFilter("bar", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter1);
    repository.save(filter2);

    final Collection<Filter> filters = repository.getFilters();

    assertThat(filters).containsExactlyInAnyOrderElementsOf(Lists.newArrayList(filter1, filter2));
  }

  @Test
  public void getFiltersShouldReturnEmptyListWhenRepositoryIsEmpty() {
    assertThat(repository.getFilters()).isEmpty();
  }

  @Test
  public void saveShouldAddFilterToRepository() {
    final BlockFilter filter = new BlockFilter("id", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final BlockFilter retrievedFilter = repository.getFilter("id", BlockFilter.class).get();

    assertThat(retrievedFilter).usingRecursiveComparison().isEqualTo(filter);
  }

  @Test
  public void saveNullFilterShouldFail() {
    final Throwable throwable = catchThrowable(() -> repository.save(null));

    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Can't save null filter");
  }

  @Test
  public void saveFilterWithSameIdShouldFail() {
    final BlockFilter filter = new BlockFilter("x", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final Throwable throwable = catchThrowable(() -> repository.save(filter));

    assertThat(throwable)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessage("Filter with id x already exists");
  }

  @Test
  public void getSingleFilterShouldReturnExistingFilterOfCorrectType() {
    final BlockFilter filter = new BlockFilter("id", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final Optional<BlockFilter> optional = repository.getFilter(filter.getId(), BlockFilter.class);

    assertThat(optional.isPresent()).isTrue();
    assertThat(optional.get()).usingRecursiveComparison().isEqualTo(filter);
  }

  @Test
  public void getSingleFilterShouldReturnEmptyForFilterOfIncorrectType() {
    final BlockFilter filter = new BlockFilter("id", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final Optional<PendingTransactionFilter> optional =
        repository.getFilter(filter.getId(), PendingTransactionFilter.class);

    assertThat(optional.isPresent()).isFalse();
  }

  @Test
  public void getSingleFilterShouldReturnEmptyForAbsentId() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final Optional<BlockFilter> optional = repository.getFilter("bar", BlockFilter.class);

    assertThat(optional.isPresent()).isFalse();
  }

  @Test
  public void getSingleFilterShouldReturnEmptyForEmptyRepository() {
    final Optional<BlockFilter> optional = repository.getFilter("id", BlockFilter.class);

    assertThat(optional.isPresent()).isFalse();
  }

  @Test
  public void getFilterCollectionShouldReturnAllFiltersOfSpecificType() {
    final BlockFilter blockFilter1 = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    final BlockFilter blockFilter2 = new BlockFilter("biz", DEFAULT_FILTER_TIMEOUT);
    final PendingTransactionFilter pendingTxFilter1 =
        new PendingTransactionFilter("bar", DEFAULT_FILTER_TIMEOUT);

    final Collection<BlockFilter> expectedFilters = Lists.newArrayList(blockFilter1, blockFilter2);

    repository.save(blockFilter1);
    repository.save(blockFilter2);
    repository.save(pendingTxFilter1);

    final Collection<BlockFilter> blockFilters = repository.getFiltersOfType(BlockFilter.class);

    assertThat(blockFilters).containsExactlyInAnyOrderElementsOf(expectedFilters);
  }

  @Test
  public void getFilterCollectionShouldReturnEmptyForNoneMatchingTypes() {
    final PendingTransactionFilter filter =
        new PendingTransactionFilter("foo", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    final Collection<BlockFilter> filters = repository.getFiltersOfType(BlockFilter.class);

    assertThat(filters).isEmpty();
  }

  @Test
  public void getFilterCollectionShouldReturnEmptyListForEmptyRepository() {
    final Collection<BlockFilter> filters = repository.getFiltersOfType(BlockFilter.class);

    assertThat(filters).isEmpty();
  }

  @Test
  public void existsShouldReturnTrueForExistingId() {
    final BlockFilter filter = new BlockFilter("id", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    assertThat(repository.exists("id")).isTrue();
  }

  @Test
  public void existsShouldReturnFalseForAbsentId() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);

    assertThat(repository.exists("bar")).isFalse();
  }

  @Test
  public void existsShouldReturnFalseForEmptyRepository() {
    assertThat(repository.exists("id")).isFalse();
  }

  @Test
  public void deleteExistingFilterShouldDeleteSuccessfully() {
    final BlockFilter filter = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter);
    repository.delete(filter.getId());

    assertThat(repository.exists(filter.getId())).isFalse();
  }

  @Test
  public void deleteAbsentFilterDoesNothing() {
    assertThat(repository.exists("foo")).isFalse();
    repository.delete("foo");
  }

  @Test
  public void deleteAllShouldClearFilters() {
    final BlockFilter filter1 = new BlockFilter("foo", DEFAULT_FILTER_TIMEOUT);
    final BlockFilter filter2 = new BlockFilter("biz", DEFAULT_FILTER_TIMEOUT);
    repository.save(filter1);
    repository.save(filter2);

    repository.deleteAll();

    assertThat(repository.exists(filter1.getId())).isFalse();
    assertThat(repository.exists(filter2.getId())).isFalse();
  }

  @Test
  public void saveShouldRejectFiltersBeyondMaxCount() {
    final FilterRepository cappedRepository = new FilterRepository(2);
    cappedRepository.save(new BlockFilter("a", DEFAULT_FILTER_TIMEOUT));
    cappedRepository.save(new BlockFilter("b", DEFAULT_FILTER_TIMEOUT));

    final Throwable throwable =
        catchThrowable(() -> cappedRepository.save(new BlockFilter("c", DEFAULT_FILTER_TIMEOUT)));

    assertThat(throwable)
        .isInstanceOf(FilterCountExceededException.class)
        .hasMessageContaining("2");
    assertThat(cappedRepository.exists("c")).isFalse();
  }

  @Test
  public void saveShouldAllowNewFilterAfterDeletionBelowMaxCount() {
    final FilterRepository cappedRepository = new FilterRepository(2);
    cappedRepository.save(new BlockFilter("a", DEFAULT_FILTER_TIMEOUT));
    cappedRepository.save(new BlockFilter("b", DEFAULT_FILTER_TIMEOUT));

    cappedRepository.delete("a");
    cappedRepository.save(new BlockFilter("c", DEFAULT_FILTER_TIMEOUT));

    assertThat(cappedRepository.exists("c")).isTrue();
  }

  @Test
  public void uncappedRepositoryShouldAllowMoreThanDefaultMaxCount() {
    final FilterRepository uncappedRepository = new FilterRepository(0);
    final int filterCount = DEFAULT_MAX_FILTER_COUNT * 2;
    for (int i = 0; i < filterCount; i++) {
      uncappedRepository.save(new BlockFilter("filter-" + i, DEFAULT_FILTER_TIMEOUT));
    }
    assertThat(uncappedRepository.getFilters()).hasSize(filterCount);
  }
}
