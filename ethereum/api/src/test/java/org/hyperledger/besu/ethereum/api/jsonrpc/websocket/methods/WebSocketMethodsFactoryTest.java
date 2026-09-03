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
package org.hyperledger.besu.ethereum.api.jsonrpc.websocket.methods;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.mock;

import org.hyperledger.besu.ethereum.api.jsonrpc.internal.methods.JsonRpcMethod;
import org.hyperledger.besu.ethereum.api.jsonrpc.websocket.subscription.SubscriptionManager;

import java.util.HashMap;
import java.util.Map;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
public class WebSocketMethodsFactoryTest {

  private WebSocketMethodsFactory factory;

  @Mock private SubscriptionManager subscriptionManager;
  private final Map<String, JsonRpcMethod> jsonRpcMethods = new HashMap<>();

  @BeforeEach
  public void before() {
    jsonRpcMethods.put("eth_unsubscribe", jsonRpcMethod());
    factory = new WebSocketMethodsFactory(subscriptionManager, jsonRpcMethods, 0);
  }

  @Test
  public void websocketsFactoryShouldCreateEthSubscribe() {
    final JsonRpcMethod method = factory.methods().get("eth_subscribe");

    assertThat(method).isNotNull();
    assertThat(method).isInstanceOf(EthSubscribe.class);
  }

  @Test
  public void websocketsFactoryShouldCreateEthUnsubscribe() {
    final JsonRpcMethod method = factory.methods().get("eth_unsubscribe");

    assertThat(method).isNotNull();
    assertThat(method).isInstanceOf(EthUnsubscribe.class);
  }

  @Test
  public void factoryCreatesExpectedNumberOfMethods() {
    final Map<String, JsonRpcMethod> methodsMap = factory.methods();
    assertThat(methodsMap).hasSize(2);
  }

  @Test
  public void factoryIncludesJsonRpcMethodsWhenCreatingWebsocketMethods() {
    final JsonRpcMethod jsonRpcMethod1 = jsonRpcMethod();
    final JsonRpcMethod jsonRpcMethod2 = jsonRpcMethod();

    final Map<String, JsonRpcMethod> jsonRpcMethodsMap = new HashMap<>();
    jsonRpcMethodsMap.put("method1", jsonRpcMethod1);
    jsonRpcMethodsMap.put("method2", jsonRpcMethod2);
    factory = new WebSocketMethodsFactory(subscriptionManager, jsonRpcMethodsMap, 0);

    final Map<String, JsonRpcMethod> methods = factory.methods();

    assertThat(methods).containsKeys("method1", "method2");
  }

  private JsonRpcMethod jsonRpcMethod() {
    return mock(JsonRpcMethod.class);
  }
}
