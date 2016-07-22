/**
 * Copyright 2015-2016 The OpenZipkin Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package zipkin.collector.kafka;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Base64;

/**
 * Decodes comma separated base64 encoded values.
 */
public class CsvBase64Decoder {
  private static final Base64.Decoder decoder = Base64.getDecoder();

  public static List<byte[]> decode(byte[] input) {
    List<byte[]> result = new ArrayList<>(16);
    int start = 0;

    for (int i = 0; i < input.length; i++) {
      if (input[i] == ',') {
        ByteBuffer raw = ByteBuffer.wrap(input, start, i - start);
        ByteBuffer decoded = decoder.decode(raw);
        result.add(getArray(decoded));
        start = i + 1;
      }
    }

    ByteBuffer raw = ByteBuffer.wrap(input, start, input.length - start);
    ByteBuffer decoded = decoder.decode(raw);
    result.add(getArray(decoded));

    return result;
  }

  // adapted from https://svn.apache.org/repos/asf/cassandra/trunk/src/java/org/apache/cassandra/utils/ByteBufferUtil.java
  static byte[] getArray(ByteBuffer buffer) {
    int length = buffer.remaining();

    if (buffer.hasArray()) {
      int boff = buffer.arrayOffset() + buffer.position();
      if (boff == 0 && length == buffer.array().length)
        return buffer.array();
      else
        return Arrays.copyOfRange(buffer.array(), boff, boff + length);
    }
    // else, DirectByteBuffer.get() is the fastest route
    byte[] bytes = new byte[length];
    buffer.duplicate().get(bytes);

    return bytes;
  }
}
