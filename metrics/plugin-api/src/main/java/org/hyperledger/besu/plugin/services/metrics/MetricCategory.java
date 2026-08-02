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

import java.util.Optional;

/**
 * A MetricCategory is used to group related metrics. Every metric belongs to one and only one
 * MetricCategory.
 *
 * <p>Categories must be registered with the {@link MetricCategoryRegistry} during plugin
 * initialisation.
 */
public interface MetricCategory {

  /**
   * Gets the name of this MetricCategory.
   *
   * @return The name of this MetricCategory.
   */
  String getName();

  /**
   * Gets the application-specific MetricCategory prefix. An empty Optional may be returned if this
   * category is not application specific.
   *
   * <p>The prefix, if present, is prepended to the category name when creating a single combined
   * name for metrics.
   *
   * @return An optional application prefix.
   */
  Optional<String> getApplicationPrefix();
}
