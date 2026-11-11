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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A plugin whose only constructor takes an argument, so that the service loader, which needs a
 * public constructor without arguments, cannot create it. A node that selects it cannot start and
 * names the plugin and its jar; a node that leaves it out of {@code --plugins} starts.
 */
@AutoService(BesuPlugin.class)
public class NoDefaultConstructorPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(NoDefaultConstructorPlugin.class);

  private final String name;

  /**
   * The only constructor.
   *
   * @param name a name nobody can supply
   */
  public NoDefaultConstructorPlugin(final String name) {
    this.name = name;
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registering {}", name);
  }

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}
}
