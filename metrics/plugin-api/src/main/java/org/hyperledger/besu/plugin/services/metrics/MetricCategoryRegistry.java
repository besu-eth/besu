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
package org.hyperledger.besu.plugin.services.metrics;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * Allow registration of {@link MetricCategory} instances so they are recognised by the metrics
 * system and can be enabled.
 *
 * <p>Categories must be registered during plugin initialisation.
 */
public interface MetricCategoryRegistry extends BesuService {

  /**
   * Registers a {@link MetricCategory}.
   *
   * @param newMetricCategory The {@link MetricCategory} that is being registered.
   */
  void addMetricCategory(final MetricCategory newMetricCategory);

  /**
   * Return true if the metrics are enabled and the metric category is enabled
   *
   * @param metricCategory the metric category
   * @return true if the metrics are enabled and the metric category is enabled
   */
  boolean isMetricCategoryEnabled(final MetricCategory metricCategory);
}
