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
package org.hyperledger.besu.ethereum.api.handlers;

import org.hyperledger.besu.ethereum.api.jsonrpc.JsonRpcConfiguration;
import org.hyperledger.besu.ethereum.api.jsonrpc.context.ContextKey;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.JsonRpcErrorResponse;
import org.hyperledger.besu.ethereum.api.jsonrpc.internal.response.RpcErrorType;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.function.BiConsumer;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonToken;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.netty.handler.codec.http.HttpResponseStatus;
import io.vertx.core.Handler;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpServerResponse;
import io.vertx.core.json.DecodeException;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.RoutingContext;

public class JsonRpcParserHandler {

  private static final JsonFactory JSON_FACTORY = new JsonFactory();
  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper(JSON_FACTORY);
  private static final int IPC_MAX_BUFFERED_BYTES =
      (int) JsonRpcConfiguration.DEFAULT_MAX_REQUEST_CONTENT_LENGTH;

  private JsonRpcParserHandler() {}

  public static Handler<Buffer> ipcHandler(
      final BiConsumer<JsonObject, JsonArray> onParsed, final Runnable onError) {
    return new IpcMessageHandler(onParsed, onError);
  }

  /**
   * Buffers socket reads until at least one complete top-level JSON document is available.
   *
   * <p>Vert.x invokes the handler once per socket read, and a single JSON-RPC request or batch can
   * span several reads. Parsing each read in isolation rejects those fragmented requests, so bytes
   * are accumulated until a top-level JSON object or array is complete.
   */
  private static final class IpcMessageHandler implements Handler<Buffer> {

    private final BiConsumer<JsonObject, JsonArray> onParsed;
    private final Runnable onError;
    private Buffer buffer = Buffer.buffer();

    private IpcMessageHandler(
        final BiConsumer<JsonObject, JsonArray> onParsed, final Runnable onError) {
      this.onParsed = onParsed;
      this.onError = onError;
    }

    @Override
    public void handle(final Buffer chunk) {
      if (chunk.length() == 0) {
        onError.run();
        return;
      }

      buffer.appendBuffer(chunk);

      if (!parseCompleteMessages()) {
        reset();
        onError.run();
        return;
      }

      if (buffer.length() > IPC_MAX_BUFFERED_BYTES) {
        // A slow or malformed client must not be able to grow the buffer without bound.
        reset();
        onError.run();
      }
    }

    /** Parses every complete JSON document currently buffered, keeping any partial suffix. */
    private boolean parseCompleteMessages() {
      final byte[] bytes = buffer.getBytes();
      int offset = 0;

      while (offset < bytes.length) {
        final int start = skipWhitespace(bytes, offset);
        if (start == bytes.length) {
          offset = start;
          break;
        }

        if (bytes[start] != '{' && bytes[start] != '[') {
          // A JSON-RPC message must be a JSON object or a batch array.
          return false;
        }

        final int end = findEndOfDocument(bytes, start);
        if (end < 0) {
          // The document is not complete yet, wait for more socket reads.
          offset = start;
          break;
        }

        if (!parseDocument(bytes, start, end)) {
          return false;
        }
        offset = end;
      }

      if (offset == bytes.length) {
        buffer = Buffer.buffer();
      } else if (offset > 0) {
        buffer = Buffer.buffer(Arrays.copyOfRange(bytes, offset, bytes.length));
      }
      return true;
    }

    private boolean parseDocument(final byte[] bytes, final int start, final int end) {
      try (ByteArrayInputStream inputStream = new ByteArrayInputStream(bytes, start, end - start);
          JsonParser parser = JSON_FACTORY.createParser(inputStream)) {

        final JsonToken token = parser.nextToken();
        if (token == JsonToken.START_OBJECT) {
          onParsed.accept(new JsonObject(OBJECT_MAPPER.readTree(parser).toString()), null);
        } else if (token == JsonToken.START_ARRAY) {
          onParsed.accept(null, new JsonArray(OBJECT_MAPPER.readTree(parser).toString()));
        }
        return true;
      } catch (IOException e) {
        return false;
      }
    }

    private void reset() {
      buffer = Buffer.buffer();
    }

    private static int skipWhitespace(final byte[] bytes, final int offset) {
      int index = offset;
      while (index < bytes.length && bytes[index] <= ' ') {
        index++;
      }
      return index;
    }

    /**
     * Returns the index just past the first complete top-level JSON object or array starting at
     * {@code start}, or {@code -1} if it is not complete yet.
     */
    private static int findEndOfDocument(final byte[] bytes, final int start) {
      int depth = 0;
      boolean inString = false;
      boolean escaped = false;

      for (int index = start; index < bytes.length; index++) {
        final byte current = bytes[index];
        if (inString) {
          if (escaped) {
            escaped = false;
          } else if (current == '\\') {
            escaped = true;
          } else if (current == '"') {
            inString = false;
          }
        } else if (current == '"') {
          inString = true;
        } else if (current == '{' || current == '[') {
          depth++;
        } else if (current == '}' || current == ']') {
          depth--;
          if (depth == 0) {
            return index + 1;
          }
          if (depth < 0) {
            return -1;
          }
        }
      }
      return -1;
    }
  }

  public static Handler<RoutingContext> handler() {
    return ctx -> {
      final HttpServerResponse response = ctx.response();
      if (ctx.body().isEmpty()) {
        errorResponse(response, RpcErrorType.PARSE_ERROR);
      } else {
        try {
          ctx.put(ContextKey.REQUEST_BODY_AS_JSON_OBJECT.name(), ctx.body().asJsonObject());
        } catch (DecodeException | ClassCastException jsonObjectDecodeException) {
          try {
            final JsonArray batchRequest = ctx.body().asJsonArray();
            if (batchRequest.isEmpty()) {
              errorResponse(response, RpcErrorType.INVALID_REQUEST);
              return;
            } else {
              ctx.put(ContextKey.REQUEST_BODY_AS_JSON_ARRAY.name(), batchRequest);
            }
          } catch (DecodeException | ClassCastException jsonArrayDecodeException) {
            errorResponse(response, RpcErrorType.PARSE_ERROR);
            return;
          }
        }
        ctx.next();
      }
    };
  }

  private static void errorResponse(
      final HttpServerResponse response, final RpcErrorType rpcError) {
    if (!response.closed()) {
      response
          .setStatusCode(HttpResponseStatus.BAD_REQUEST.code())
          .end(Json.encode(new JsonRpcErrorResponse(null, rpcError)));
    }
  }
}
