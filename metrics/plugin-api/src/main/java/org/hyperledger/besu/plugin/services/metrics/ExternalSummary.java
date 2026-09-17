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
package org.hyperledger.besu.plugin.services.metrics;

import java.util.List;

/**
 * Record of summary data the is kept outside the metric system. Useful when existing libraries
 * calculate the summary data on their own, and we want to export that summary via the configured
 * metric system. A notable example are RocksDB statistics.
 *
 * @param count the number of observations
 * @param sum the sum of all the observations
 * @param quantiles a list of quantiles with values
 */
public record ExternalSummary(long count, double sum, List<Quantile> quantiles) {
  /**
   * Represent a single quantile and its value
   *
   * @param quantile the quantile
   * @param value the value
   */
  public record Quantile(double quantile, double value) {}
}
