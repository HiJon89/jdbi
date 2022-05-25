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

import java.util.List;

import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.sqlobject.batch.BatchAccumulator.IntArrayAccumulator;
import org.skife.jdbi.v2.sqlobject.batch.BatchAccumulator.IntListAccumulator;
import org.skife.jdbi.v2.sqlobject.batch.BatchAccumulator.LongListAccumulator;
import org.skife.jdbi.v2.tweak.ResultColumnMapper;
import org.skife.jdbi.v2.util.IntegerColumnMapper;
import org.skife.jdbi.v2.util.LongColumnMapper;

public abstract class BatchReturner<T> {

  public abstract T executeBatch(PreparedBatch batch);

  public abstract BatchAccumulator newAccumulator();

  public static BatchReturner<int[]> newIntArrayReturner() {
    return new BatchReturner<int[]>() {
      @Override
      public int[] executeBatch(PreparedBatch batch) {
        return batch.execute();
      }

      @Override
      public BatchAccumulator newAccumulator() {
        return new IntArrayAccumulator();
      }
    };
  }

  public static BatchReturner<List<Integer>> newGeneratedIntReturner(String columnName) {
    return new GeneratedKeysReturner<Integer>(IntegerColumnMapper.PRIMITIVE, columnName) {
      @Override
      public BatchAccumulator newAccumulator() {
        return new IntListAccumulator();
      }
    };
  }

  public static BatchReturner<List<Long>> newGeneratedLongReturner(String columnName) {
    return new GeneratedKeysReturner<Long>(LongColumnMapper.PRIMITIVE, columnName) {
      @Override
      public BatchAccumulator newAccumulator() {
        return new LongListAccumulator();
      }
    };
  }

  private abstract static class GeneratedKeysReturner<T> extends BatchReturner<List<T>> {
    private final ResultColumnMapper<T> resultColumnMapper;
    private final String columnName;

    GeneratedKeysReturner(
        ResultColumnMapper<T> resultColumnMapper,
        String columnName
    ) {
      this.resultColumnMapper = resultColumnMapper;
      this.columnName = columnName;
    }

    public List<T> executeBatch(PreparedBatch batch) {
      if (columnName.isEmpty()) {
        return batch.executeAndGenerateKeys(resultColumnMapper).list();
      } else {
        return batch.executeAndGenerateKeys(resultColumnMapper, columnName).list();
      }
    }
  }
}
