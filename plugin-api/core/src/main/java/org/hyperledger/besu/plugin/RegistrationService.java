/*
 * Copyright contributors to Hyperledger Besu.
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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.BesuService;

/**
 * Marks a Besu-provided service that is usable in {@link BesuPlugin#register(RegistrationContext)}.
 *
 * <p>Registration is a window that closes: what a plugin registers through these services is read
 * by Besu once, after the last plugin has returned from {@code register()}, so this marker does not
 * widen into the later phases. A service that is also readable later additionally implements {@link
 * StartService}.
 */
public interface RegistrationService extends BesuService {}
