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
 * A plugin whose static initializer always throws: the class cannot be initialized, so a node that
 * selects it must fail naming the cause, and a node that leaves it out of {@code --plugins} must
 * never initialize it.
 */
@AutoService(BesuPlugin.class)
public class BrokenStaticInitializerPlugin implements BesuPlugin {
  private static final Logger LOG = LoggerFactory.getLogger(BrokenStaticInitializerPlugin.class);
  private static final String REASON = reason();

  private static String reason() {
    throw new IllegalStateException("I was told to fail in the static initializer");
  }

  @Override
  public void register(final RegistrationContext context) {
    LOG.info("Registering, reason {}", REASON);
  }

  @Override
  public void start(final StartContext context) {}

  @Override
  public void stop() {}
}
