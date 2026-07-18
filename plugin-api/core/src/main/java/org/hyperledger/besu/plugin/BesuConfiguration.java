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
package org.hyperledger.besu.plugin;

import org.hyperledger.besu.plugin.services.BesuService;

import java.nio.file.Path;

/** Core configuration of the Besu node. */
@Unstable
public interface BesuConfiguration extends BesuService {

  /**
   * Location of the data directory in the file system running the client.
   *
   * @return location of the data directory in the file system of the client.
   */
  Path getDataPath();
}
