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
package org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.engine;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.QosTimer;

import com.google.common.annotations.VisibleForTesting;
import io.vertx.core.Vertx;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EngineQosTimer implements EngineCallListener {
  static final long QOS_TIMEOUT_MILLIS = 120000L;
  private static final Logger LOG = LoggerFactory.getLogger(EngineQosTimer.class);

  private final QosTimer qosTimer;

  public EngineQosTimer(final Vertx vertx) {
    qosTimer = new QosTimer(vertx, QOS_TIMEOUT_MILLIS, lastCall -> logTimeoutWarning());
    qosTimer.resetTimer();
  }

  @Override
  public void executionEngineCalled() {
    getQosTimer().resetTimer();
  }

  public void logTimeoutWarning() {
    LOG.warn(
        "Execution engine not called in {} seconds, consensus client may not be connected",
        QOS_TIMEOUT_MILLIS / 1000L);
  }

  @Override
  public void stop() {
    qosTimer.stop();
  }

  @VisibleForTesting
  public QosTimer getQosTimer() {
    return qosTimer;
  }
}
