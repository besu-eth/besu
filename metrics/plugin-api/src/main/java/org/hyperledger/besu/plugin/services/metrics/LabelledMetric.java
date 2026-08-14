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

/**
 * A metric with labels associated. Values for the associated labels can be provided to access the
 * underlying metric.
 *
 * @param <T> The type of metric the labels are applied to.
 */
public interface LabelledMetric<T> {

  /**
   * Returns a metric tagged with the specified label values.
   *
   * @param labels An array of label values in the same order as the labels when creating this
   *     metric. The number of values provided must match the number of labels.
   * @return A metric tagged with the specified labels.
   */
  T labels(String... labels);
}
