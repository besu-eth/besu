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

import org.hyperledger.besu.plugin.RegistrationService;

/**
 * Allow registration of {@link MetricCategory} instances so they are recognised by the metrics
 * system and can be enabled.
 *
 * <p>Categories must be registered from {@link org.hyperledger.besu.plugin.BesuPlugin#register}:
 * the {@code --metrics-category} option, which may name them, is validated and the metrics system
 * built after registration has closed.
 */
public interface MetricCategoryRegistry extends RegistrationService {

  /**
   * Registers a {@link MetricCategory}.
   *
   * @param newMetricCategory The {@link MetricCategory} that is being registered.
   * @throws org.hyperledger.besu.plugin.RegistrationClosedException if called after registration
   *     has closed
   */
  void addMetricCategory(final MetricCategory newMetricCategory);

  /**
   * Return true if the metrics are enabled and the metric category is enabled
   *
   * <p>The answer depends on the metrics configuration, which Besu sets only after registration has
   * closed, so do not call this from {@link org.hyperledger.besu.plugin.BesuPlugin#register}. From
   * {@code start()} onwards, {@link
   * org.hyperledger.besu.plugin.services.MetricsSystem#isCategoryEnabled} gives the same answer.
   *
   * @param metricCategory the metric category
   * @return true if the metrics are enabled and the metric category is enabled
   */
  boolean isMetricCategoryEnabled(final MetricCategory metricCategory);
}
