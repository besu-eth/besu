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
package org.hyperledger.besu.cli.options;

/** Supported structured logging formats for the --logging-format CLI option. */
public enum LoggingFormat {
  /** Traditional pattern-based console logging (default). */
  PLAIN(null),

  /** Elastic Common Schema JSON format. */
  ECS("log4j2-ecs.xml"),

  /** Google Cloud Platform JSON format. */
  GCP("log4j2-gcp.xml"),

  /** Logstash JSON Event Layout V1. */
  LOGSTASH("log4j2-logstash.xml"),

  /** Graylog Extended Log Format. */
  GELF("log4j2-gelf.xml");

  private final String configResourceName;

  LoggingFormat(final String configResourceName) {
    this.configResourceName = configResourceName;
  }

  /**
   * Gets the classpath resource name of the bundled Log4j2 configuration file for this format.
   *
   * @return the resource name, or null for PLAIN which uses the default log4j2.xml
   */
  public String getConfigResourceName() {
    return configResourceName;
  }
}
