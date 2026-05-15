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
package org.hyperledger.besu.config;

import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

import org.hyperledger.besu.datatypes.Address;

import java.util.HashMap;
import java.util.Map;

import com.fasterxml.jackson.databind.node.ObjectNode;
import org.junit.jupiter.api.Test;

public class JsonBftConfigOptionsTest {

  private static final int EXPECTED_DEFAULT_EPOCH_LENGTH = 30_000;
  private static final int EXPECTED_DEFAULT_BLOCK_PERIOD = 1;
  private static final int EXPECTED_EMPTY_DEFAULT_BLOCK_PERIOD = 0;
  private static final int EXPECTED_DEFAULT_REQUEST_TIMEOUT = 1;
  private static final int EXPECTED_DEFAULT_GOSSIPED_HISTORY_LIMIT = 1000;
  private static final int EXPECTED_DEFAULT_MESSAGE_QUEUE_LIMIT = 1000;
  private static final int EXPECTED_DEFAULT_DUPLICATE_MESSAGE_LIMIT = 100;
  private static final int EXPECTED_DEFAULT_FUTURE_MESSAGES_LIMIT = 1000;
  private static final int EXPECTED_DEFAULT_FUTURE_MESSAGES_MAX_DISTANCE = 10;

  @Test
  public void shouldGetEpochLengthFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("epochlength", 10_000));
    assertThat(config.getEpochLength()).isEqualTo(10_000);
  }

  @Test
  public void shouldFallbackToDefaultEpochLength() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getEpochLength()).isEqualTo(EXPECTED_DEFAULT_EPOCH_LENGTH);
  }

  @Test
  public void shouldGetDefaultEpochLengthFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getEpochLength())
        .isEqualTo(EXPECTED_DEFAULT_EPOCH_LENGTH);
  }

  @Test
  public void shouldGetBlockPeriodFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("blockperiodseconds", 5));
    assertThat(config.getBlockPeriodSeconds()).isEqualTo(5);
  }

  @Test
  public void shouldGetEmptyBlockPeriodFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("emptyblockperiodseconds", 60));
    assertThat(config.getEmptyBlockPeriodSeconds()).isEqualTo(60);
  }

  @Test
  public void shouldGetEmptyBlockPeriodFromDeprecatedConfigKey() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("xemptyblockperiodseconds", 60));
    assertThat(config.getEmptyBlockPeriodSeconds()).isEqualTo(60);
  }

  @Test
  public void shouldPreferNewEmptyBlockPeriodOverDeprecatedWhenBothSet() {
    final Map<String, Object> options = new HashMap<>();
    options.put("emptyblockperiodseconds", 60);
    options.put("xemptyblockperiodseconds", 30);
    final BftConfigOptions config = fromConfigOptions(options);
    assertThat(config.getEmptyBlockPeriodSeconds()).isEqualTo(60);
  }

  @Test
  public void shouldFallbackToDefaultBlockPeriod() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getBlockPeriodSeconds()).isEqualTo(EXPECTED_DEFAULT_BLOCK_PERIOD);
  }

  @Test
  public void shouldFallbackToEmptyDefaultBlockPeriod() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getEmptyBlockPeriodSeconds()).isEqualTo(EXPECTED_EMPTY_DEFAULT_BLOCK_PERIOD);
  }

  @Test
  public void shouldGetDefaultBlockPeriodFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getBlockPeriodSeconds())
        .isEqualTo(EXPECTED_DEFAULT_BLOCK_PERIOD);
  }

  @Test
  public void shouldGetDefaultEmptyBlockPeriodFromDefaultConfig() {

    assertThat(JsonBftConfigOptions.DEFAULT.getEmptyBlockPeriodSeconds())
        .isEqualTo(EXPECTED_EMPTY_DEFAULT_BLOCK_PERIOD);
  }

  @Test
  public void shouldThrowOnNonPositiveBlockPeriod() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("blockperiodseconds", -1));
    assertThatThrownBy(() -> config.getBlockPeriodSeconds())
        .isInstanceOf(IllegalArgumentException.class);
  }

  @Test
  public void shouldNotThrowOnNonPositiveEmptyBlockPeriod() {
    // can be 0 to be compatible with older versions
    final BftConfigOptions config = fromConfigOptions(singletonMap("emptyblockperiodseconds", 0));
    assertThatCode(() -> config.getEmptyBlockPeriodSeconds()).doesNotThrowAnyException();
  }

  @Test
  public void shouldNotThrowOnNonPositiveEmptyBlockPeriodForDeprecatedKey() {
    // can be 0 to be compatible with older versions
    final BftConfigOptions config = fromConfigOptions(singletonMap("xemptyblockperiodseconds", 0));
    assertThatCode(() -> config.getEmptyBlockPeriodSeconds()).doesNotThrowAnyException();
  }

  @Test
  public void shouldGetRequestTimeoutFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("requesttimeoutseconds", 5));
    assertThat(config.getRequestTimeoutSeconds()).isEqualTo(5);
  }

  @Test
  public void shouldFallbackToDefaultRequestTimeout() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getRequestTimeoutSeconds()).isEqualTo(EXPECTED_DEFAULT_REQUEST_TIMEOUT);
  }

  @Test
  public void shouldGetDefaultRequestTimeoutFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getRequestTimeoutSeconds())
        .isEqualTo(EXPECTED_DEFAULT_REQUEST_TIMEOUT);
  }

  @Test
  public void shouldGetGossipedHistoryLimitFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("gossipedhistorylimit", 100));
    assertThat(config.getGossipedHistoryLimit()).isEqualTo(100);
  }

  @Test
  public void shouldFallbackToDefaultGossipedHistoryLimit() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getGossipedHistoryLimit()).isEqualTo(EXPECTED_DEFAULT_GOSSIPED_HISTORY_LIMIT);
  }

  @Test
  public void shouldGetDefaultGossipedHistoryLimitFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getGossipedHistoryLimit())
        .isEqualTo(EXPECTED_DEFAULT_GOSSIPED_HISTORY_LIMIT);
  }

  @Test
  public void shouldGetMessageQueueLimitFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("messagequeuelimit", 100));
    assertThat(config.getMessageQueueLimit()).isEqualTo(100);
  }

  @Test
  public void shouldFallbackToDefaultMessageQueueLimit() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getMessageQueueLimit()).isEqualTo(EXPECTED_DEFAULT_MESSAGE_QUEUE_LIMIT);
  }

  @Test
  public void shouldGetDefaultMessageQueueLimitFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getMessageQueueLimit())
        .isEqualTo(EXPECTED_DEFAULT_MESSAGE_QUEUE_LIMIT);
  }

  @Test
  public void shouldGetDuplicateMessageLimitFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("duplicatemessagelimit", 50));
    assertThat(config.getDuplicateMessageLimit()).isEqualTo(50);
  }

  @Test
  public void shouldFallbackToDefaultDuplicateMessageLimit() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getDuplicateMessageLimit())
        .isEqualTo(EXPECTED_DEFAULT_DUPLICATE_MESSAGE_LIMIT);
  }

  @Test
  public void shouldGetDefaultDuplicateMessageLimitFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getDuplicateMessageLimit())
        .isEqualTo(EXPECTED_DEFAULT_DUPLICATE_MESSAGE_LIMIT);
  }

  @Test
  public void shouldGetFutureMessagesLimitFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("futuremessageslimit", 50));
    assertThat(config.getFutureMessagesLimit()).isEqualTo(50);
  }

  @Test
  public void shouldFallbackToDefaultFutureMessagesLimit() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getFutureMessagesLimit()).isEqualTo(EXPECTED_DEFAULT_FUTURE_MESSAGES_LIMIT);
  }

  @Test
  public void shouldGetDefaultFutureMessagesLimitsFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getFutureMessagesLimit())
        .isEqualTo(EXPECTED_DEFAULT_FUTURE_MESSAGES_LIMIT);
  }

  @Test
  public void shouldGetFutureMessagesMaxDistanceFromConfig() {
    final BftConfigOptions config =
        fromConfigOptions(singletonMap("futuremessagesmaxdistance", 50));
    assertThat(config.getFutureMessagesMaxDistance()).isEqualTo(50);
  }

  @Test
  public void shouldFallbackToDefaultFutureMessagesMaxDistance() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getFutureMessagesMaxDistance())
        .isEqualTo(EXPECTED_DEFAULT_FUTURE_MESSAGES_MAX_DISTANCE);
  }

  @Test
  public void shouldGetDefaultFutureMessagesMaxDistanceFromDefaultConfig() {
    assertThat(JsonBftConfigOptions.DEFAULT.getFutureMessagesMaxDistance())
        .isEqualTo(EXPECTED_DEFAULT_FUTURE_MESSAGES_MAX_DISTANCE);
  }

  @Test
  public void shouldGetMiningBeneficiaryFromConfig() {
    final Address miningBeneficiary =
        Address.fromHexString("0xfe3b557e8fb62b89f4916b721be55ceb828dbd73");
    final BftConfigOptions config =
        fromConfigOptions(singletonMap("miningbeneficiary", miningBeneficiary.toString()));
    assertThat(config.getMiningBeneficiary()).contains(miningBeneficiary);
  }

  @Test
  public void shouldGetEmptyMiningBeneficiaryFromConfig() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("miningbeneficiary", " "));
    assertThat(config.getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void shouldFallbackToDefaultEmptyMiningBeneficiary() {
    final BftConfigOptions config = fromConfigOptions(emptyMap());
    assertThat(config.getMiningBeneficiary()).isEmpty();
  }

  @Test
  public void shouldThrowOnInvalidMiningBeneficiary() {
    final BftConfigOptions config = fromConfigOptions(singletonMap("miningbeneficiary", "bla"));
    assertThatThrownBy(config::getMiningBeneficiary)
        .isInstanceOf(IllegalArgumentException.class)
        .hasMessageContaining("Mining beneficiary in config is not a valid ethereum address");
  }

  private BftConfigOptions fromConfigOptions(final Map<String, Object> ibftConfigOptions) {
    final ObjectNode rootNode = JsonUtil.createEmptyObjectNode();
    final ObjectNode configNode = JsonUtil.createEmptyObjectNode();
    final ObjectNode options = JsonUtil.objectNodeFromMap(ibftConfigOptions);
    configNode.set("ibft2", options);
    rootNode.set("config", configNode);
    return GenesisConfig.fromConfig(rootNode).getConfigOptions().getBftConfigOptions();
  }
}
