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
package org.hyperledger.besu.plugin.services;

/**
 * Root of every service a plugin can look up or publish.
 *
 * <p>Besu-provided services additionally implement one of the tier markers ({@link
 * org.hyperledger.besu.plugin.RegistrationService}, {@link
 * org.hyperledger.besu.plugin.StartService}, {@link org.hyperledger.besu.plugin.RunningService}),
 * which state the earliest lifecycle phase in which the service works and bound the lookup of that
 * phase's context. A service published by a plugin through {@link
 * org.hyperledger.besu.plugin.RegistrationContext#registerService(Class, BesuService)} implements
 * this interface directly.
 */
public interface BesuService {}
