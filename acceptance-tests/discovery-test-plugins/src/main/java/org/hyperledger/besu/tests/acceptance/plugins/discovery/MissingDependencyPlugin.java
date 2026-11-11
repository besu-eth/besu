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
import org.junit.jupiter.api.function.Executable;

/**
 * A plugin that implements an interface of a library the node does not have, as a plugin does whose
 * library jar was not copied into the plugins directory: its class cannot be loaded. A node that
 * selects it cannot start and names the plugin, its jar and the missing class; a node that leaves
 * it out of {@code --plugins} starts.
 */
@AutoService(BesuPlugin.class)
public class MissingDependencyPlugin implements BesuPlugin, Executable {

  @Override
  public void execute() {}

  @Override
  public void register(final RegistrationContext context) {}

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}
}
