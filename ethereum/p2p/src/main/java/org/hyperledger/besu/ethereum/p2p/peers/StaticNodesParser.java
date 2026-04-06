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
package org.hyperledger.besu.ethereum.p2p.peers;

import static com.google.common.base.Preconditions.checkArgument;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Collections.emptySet;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.AccessDeniedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;

import io.vertx.core.json.DecodeException;
import io.vertx.core.json.JsonArray;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StaticNodesParser {

  private static final Logger LOG = LoggerFactory.getLogger(StaticNodesParser.class);

  public static Set<EnodeURLImpl> fromPath(
      final Path path, final EnodeDnsConfiguration enodeDnsConfiguration)
      throws IOException, IllegalArgumentException {

    try {
      return readEnodesFromPath(path, enodeDnsConfiguration);
    } catch (FileNotFoundException | NoSuchFileException ex) {
      LOG.debug("StaticNodes file {} does not exist, no static connections will be created.", path);
      return emptySet();
    } catch (AccessDeniedException ex) {
      LOG.warn(
          "Access denied to static nodes file ({}). Ensure static nodes file and node data directory have correct permissions.",
          path);
      throw ex;
    } catch (IOException ex) {
      LOG.warn("Unable to parse static nodes file ({})", path);
      throw ex;
    } catch (DecodeException ex) {
      LOG.warn("Content of ({}} was invalid json, and could not be decoded.", path);
      throw ex;
    } catch (IllegalArgumentException ex) {
      LOG.warn("Parsing ({}) has failed due incorrectly formatted enode element.", path);
      throw ex;
    }
  }

  private static Set<EnodeURLImpl> readEnodesFromPath(
      final Path path, final EnodeDnsConfiguration enodeDnsConfiguration) throws IOException {
    final byte[] staticNodesContent = Files.readAllBytes(path);
    if (staticNodesContent.length == 0) {
      return emptySet();
    }

    final JsonArray enodeJsonArray = new JsonArray(new String(staticNodesContent, UTF_8));
    return enodeJsonArray.stream()
        .map(obj -> decodeString((String) obj, enodeDnsConfiguration))
        .collect(Collectors.toSet());
  }

  private static EnodeURLImpl decodeString(
      final String input, final EnodeDnsConfiguration enodeDnsConfiguration) {
    try {
      final EnodeURLImpl enode = EnodeURLImpl.fromString(input, enodeDnsConfiguration);
      checkArgument(
          enode.isListening(), "Static node must be configured with a valid listening port.");
      return enode;
    } catch (IllegalArgumentException ex) {
      throw new IllegalArgumentException("Illegal static enode supplied (" + input + ")", ex);
    }
  }
}
