/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.skife.jdbi.v2.sqlobject.batch;

import java.util.ArrayList;
import java.util.List;

public abstract class BatchAccumulator {
  protected final List<Object> values = new ArrayList<Object>();

  public void add(Object value) {
    values.add(value);
  }

  public abstract Object getResult();

  static class IntArrayAccumulator extends BatchAccumulator {

    @Override
    public int[] getResult() {
      int length = 0;
      for (Object value : values) {
        if (value instanceof int[]) {
          length += ((int []) value).length;
        } else {
          throw new IllegalStateException("non-int[] value in result set: " + value);
        }
      }

      int[] result = new int[length];
      int offset = 0;
      for (Object value : values) {
        int[] valueArray = (int[]) value;
        System.arraycopy(valueArray, 0, result, offset, valueArray.length);
        offset += valueArray.length;
      }
      return result;
    }
  }

  private abstract static class ListAccumulator extends BatchAccumulator {

    @Override
    public void add(Object value) {
      if (value instanceof List) {
        values.addAll((List<?>) value);
      } else {
        throw new IllegalArgumentException("Expected List, got " + value);
      }
    }
  }

  static class IntListAccumulator extends ListAccumulator {

    @Override
    public int[] getResult() {
      int[] result = new int[values.size()];

      int i = 0;
      for (Object value : values) {
        if (value instanceof Integer) {
          result[i++] = (Integer) value;
        } else {
          throw new IllegalStateException("non-Integer value in result set: " + value);
        }
      }

      return result;
    }
  }

  static class LongListAccumulator extends ListAccumulator {

    @Override
    public long[] getResult() {
      long[] result = new long[values.size()];

      int i = 0;
      for (Object value : values) {
        if (value instanceof Long) {
          result[i++] = (Long) value;
        } else {
          throw new IllegalStateException("non-Long value in result set: " + value);
        }
      }

      return result;
    }
  }
}
