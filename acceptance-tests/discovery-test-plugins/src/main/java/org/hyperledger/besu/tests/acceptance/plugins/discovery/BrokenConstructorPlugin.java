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
package org.hyperledger.besu.tests.acceptance.plugins.discovery;

import org.hyperledger.besu.plugin.BesuPlugin;
import org.hyperledger.besu.plugin.RegistrationContext;
import org.hyperledger.besu.plugin.StartContext;

import com.google.auto.service.AutoService;

/**
 * A plugin whose constructor always throws: a node that selects it cannot start, and a node that
 * leaves it out of {@code --plugins} must never run it.
 */
@AutoService(BesuPlugin.class)
public class BrokenConstructorPlugin implements BesuPlugin {

  /** Fails. */
  public BrokenConstructorPlugin() {
    throw new IllegalStateException("I was told to fail in the constructor");
  }

  @Override
  public void register(final RegistrationContext context) {}

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}
}
