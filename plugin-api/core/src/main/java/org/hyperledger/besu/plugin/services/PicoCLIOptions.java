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
package org.hyperledger.besu.plugin.services;

/**
 * A service that plugins can use to add CLI options and commands to the BesuCommand. The PicoCLI
 * library annotations will be inspected and the object will be passed into a
 * picocli.CommandLine.addMixin call.
 *
 * <p>This service will be available during the registration callbacks.
 *
 * <p>CLI arguments should conform to the <a
 * href="https://github.com/hyperledger/besu/blob/master/CLI-STYLE-GUIDE.md">CLI-STYLE-GUIDE.md</a>
 * conventions.
 */
public interface PicoCLIOptions extends BesuService {

  /**
   * During the registration callback plugins can register CLI options that should be added to
   * Besu's CLI startup.
   *
   * @param namespace A namespace prefix. All registered options must start with this prefix
   * @param optionObject The instance of the object to be inspected. PicoCLI will reflect the fields
   *     of this object to extract the CLI options.
   */
  void addPicoCLIOptions(String namespace, Object optionObject);
}
