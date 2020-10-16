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
package org.skife.jdbi.v2.sqlobject;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;

import org.skife.jdbi.v2.ConcreteStatementContext;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.PreparedBatch;
import org.skife.jdbi.v2.PreparedBatchPart;
import org.skife.jdbi.v2.StatementContext;
import org.skife.jdbi.v2.TransactionCallback;
import org.skife.jdbi.v2.TransactionStatus;
import org.skife.jdbi.v2.exceptions.UnableToCreateSqlObjectException;
import org.skife.jdbi.v2.exceptions.UnableToExecuteStatementException;
import org.skife.jdbi.v2.sqlobject.customizers.BatchChunkSize;
import org.skife.jdbi.v2.util.IntegerColumnMapper;
import org.skife.jdbi.v2.util.LongColumnMapper;

import com.fasterxml.classmate.members.ResolvedMethod;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import net.sf.cglib.proxy.MethodProxy;

class BatchHandler extends CustomizingStatementHandler
{
    private final String  sql;
    private final boolean transactional;
    private final ChunkSizeFunction batchChunkSize;
    private final IntegerReturner integerReturner;
    private final LongReturner longReturner;

    BatchHandler(Class<?> sqlObjectType, ResolvedMethod method)
    {
        super(sqlObjectType, method);
        if(!returnTypeIsValid(method.getRawMember().getReturnType()) ) {
            throw new UnableToCreateSqlObjectException(invalidReturnTypeMessage(method));
        }
        Method raw_method = method.getRawMember();
        SqlBatch anno = raw_method.getAnnotation(SqlBatch.class);
        this.sql = SqlObject.getSql(anno, raw_method);
        this.transactional = anno.transactional();
        this.batchChunkSize = determineBatchChunkSize(sqlObjectType, raw_method);
        final GetGeneratedKeys getGeneratedKeys = raw_method.getAnnotation(GetGeneratedKeys.class);
        if (getGeneratedKeys == null) {
            integerReturner = new IntegerReturner()
            {
                @Override
                public int[] value(PreparedBatch batch)
                {
                    return batch.execute();
                }
            };
            longReturner = null;
        }
        else if (getGeneratedKeys.columnName().isEmpty()) {
            if (method.getReturnType().getErasedType().equals(long[].class)) {
                longReturner = new LongReturner()
                {
                    @Override
                    public long[] value(PreparedBatch batch)
                    {
                        return toPrimitiveLongArray(batch.executeAndGenerateKeys(LongColumnMapper.PRIMITIVE).list());
                    }
                };
                integerReturner = null;
            } else {
                integerReturner = new IntegerReturner()
                {
                    @Override
                    public int[] value(PreparedBatch batch)
                    {
                        return toPrimitiveArray(batch.executeAndGenerateKeys(IntegerColumnMapper.PRIMITIVE).list());
                    }
                };
                longReturner = null;
            }
        }
        else {
            if (method.getReturnType().getErasedType().equals(long[].class)) {
                longReturner = new LongReturner()
                {
                    @Override
                    public long[] value(PreparedBatch batch)
                    {
                        String columnName = getGeneratedKeys.columnName();
                        return toPrimitiveLongArray(batch.executeAndGenerateKeys(LongColumnMapper.PRIMITIVE, columnName).list());
                    }
                };
                integerReturner = null;
            } else {
                integerReturner = new IntegerReturner()
                {
                    @Override
                    public int[] value(PreparedBatch batch)
                    {
                        String columnName = getGeneratedKeys.columnName();
                        return toPrimitiveArray(batch.executeAndGenerateKeys(IntegerColumnMapper.PRIMITIVE, columnName).list());
                    }
                };
                longReturner = null;
            }
        }
    }

    private ChunkSizeFunction determineBatchChunkSize(Class<?> sqlObjectType, Method raw_method)
    {
        // this next big if chain determines the batch chunk size. It looks from most specific
        // scope to least, that is: as an argument, then on the method, then on the class,
        // then default to Integer.MAX_VALUE

        int index_of_batch_chunk_size_annotation_on_parameter;
        if ((index_of_batch_chunk_size_annotation_on_parameter = findBatchChunkSizeFromParam(raw_method)) >= 0) {
            return new ParamBasedChunkSizeFunction(index_of_batch_chunk_size_annotation_on_parameter);
        }
        else if (raw_method.isAnnotationPresent(BatchChunkSize.class)) {
            final int size = raw_method.getAnnotation(BatchChunkSize.class).value();
            if (size <= 0) {
                throw new IllegalArgumentException("Batch chunk size must be >= 0");
            }
            return new ConstantChunkSizeFunction(size);
        }
        else if (sqlObjectType.isAnnotationPresent(BatchChunkSize.class)) {
            final int size = BatchChunkSize.class.cast(sqlObjectType.getAnnotation(BatchChunkSize.class)).value();
            return new ConstantChunkSizeFunction(size);
        }
        else {
            return new ConstantChunkSizeFunction(Integer.MAX_VALUE);
        }
    }

    private int findBatchChunkSizeFromParam(Method raw_method)
    {
        Annotation[][] param_annos = raw_method.getParameterAnnotations();
        for (int i = 0; i < param_annos.length; i++) {
            Annotation[] annos = param_annos[i];
            for (Annotation anno : annos) {
                if (anno.annotationType().isAssignableFrom(BatchChunkSize.class)) {
                    return i;
                }
            }
        }
        return -1;
    }

    @Override
    public Object invoke(HandleDing h, Object target, Object[] args, MethodProxy mp)
    {
        boolean foundIterator = false;
        Handle handle = h.getHandle();

        List<Iterator> extras = new ArrayList<Iterator>();
        for (final Object arg : args) {
            if (arg instanceof Iterable) {
                extras.add(((Iterable) arg).iterator());
                foundIterator = true;
            }
            else if (arg instanceof Iterator) {
                extras.add((Iterator) arg);
                foundIterator = true;
            }
            else if (arg.getClass().isArray()) {
                extras.add(Arrays.asList((Object[])arg).iterator());
                foundIterator = true;
            }
            else {
                extras.add(new Iterator()
                {
                    @Override
                    public boolean hasNext()
                    {
                        return true;
                    }

                    @Override
                    @SuppressFBWarnings("IT_NO_SUCH_ELEMENT")
                    public Object next()
                    {
                        return arg;
                    }

                    @Override
                    public void remove()
                    {
                        // NOOP
                    }
                }
                );
            }
        }

        if (!foundIterator) {
            throw new UnableToExecuteStatementException("@SqlBatch must have at least one iterable parameter", (StatementContext)null);
        }

        int processed = 0;
        List<int[]> rs_parts = new ArrayList<int[]>();
        List<long[]> rs_parts_long = new ArrayList<long[]>();

        PreparedBatch batch = handle.prepareBatch(sql);
        populateSqlObjectData((ConcreteStatementContext) batch.getContext());
        applyCustomizers(batch, args);
        Object[] _args;
        int chunk_size = batchChunkSize.call(args);

        while ((_args = next(extras)) != null) {
            PreparedBatchPart part = batch.add();
            applyBinders(part, _args);

            if (++processed == chunk_size) {
                // execute this chunk
                processed = 0;
                if(integerReturner != null) {
                    rs_parts.add(executeBatch(handle, batch));
                } else {
                    rs_parts_long.add(executeLongBatch(handle, batch));
                }
                batch = handle.prepareBatch(sql);
                populateSqlObjectData((ConcreteStatementContext) batch.getContext());
                applyCustomizers(batch, args);
            }
        }

        //execute the rest
        if(integerReturner != null) {
            rs_parts.add(executeBatch(handle, batch));
        } else {
            rs_parts_long.add(executeLongBatch(handle, batch));
        }

        // combine results
        if (integerReturner != null) {
        int end_size = 0;
        for (int[] rs_part : rs_parts) {
            end_size += rs_part.length;
        }
        int[] rs = new int[end_size];
        int offset = 0;
        for (int[] rs_part : rs_parts) {
            System.arraycopy(rs_part, 0, rs, offset, rs_part.length);
            offset += rs_part.length;
        }
        return rs;
        } else {
          int end_size = 0;
          for (long[] rs_part : rs_parts_long) {
            end_size += rs_part.length;
          }
          long[] rs = new long[end_size];
          int offset = 0;
          for (long[] rs_part : rs_parts_long) {
            System.arraycopy(rs_part, 0, rs, offset, rs_part.length);
            offset += rs_part.length;
          }
          return rs;
        }
    }

    private int[] executeBatch(final Handle handle, final PreparedBatch batch)
    {
        if (!handle.isInTransaction() && transactional) {
            // it is safe to use same prepared batch as the inTransaction passes in the same
            // Handle instance.
            return handle.inTransaction(new TransactionCallback<int[]>()
            {
                @Override
                public int[] inTransaction(Handle conn, TransactionStatus status) throws Exception
                {
                    return integerReturner.value(batch);
                }
            });
        }
        else {
            return integerReturner.value(batch);
        }
    }

    private long[] executeLongBatch(final Handle handle, final PreparedBatch batch)
    {
        if (!handle.isInTransaction() && transactional) {
            // it is safe to use same prepared batch as the inTransaction passes in the same
            // Handle instance.
            return handle.inTransaction(new TransactionCallback<long[]>()
            {
                @Override
                public long[] inTransaction(Handle conn, TransactionStatus status) throws Exception
                {
                    return longReturner.value(batch);
                }
            });
        }
        else {
            return longReturner.value(batch);
        }
    }

    private static int[] toPrimitiveArray(List<Integer> list)
    {
        int[] array = new int[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    private static long[] toPrimitiveLongArray(List<Long> list)
    {
        long[] array = new long[list.size()];
        for (int i = 0; i < list.size(); i++) {
            array[i] = list.get(i);
        }

        return array;
    }

    private static Object[] next(List<Iterator> args)
    {
        List<Object> rs = new ArrayList<Object>();
        for (Iterator arg : args) {
            if (arg.hasNext()) {
                rs.add(arg.next());
            }
            else {
                return null;
            }
        }
        return rs.toArray();
    }

    private interface IntegerReturner
    {
        String type = Integer.TYPE.getName();
        int[] value(PreparedBatch batch);
    }

    private interface LongReturner
    {
        String type = Long.TYPE.getName();
        long[] value(PreparedBatch batch);
    }

    private interface ChunkSizeFunction
    {
        int call(Object[] args);
    }

    private static class ConstantChunkSizeFunction implements ChunkSizeFunction
    {
        private final int value;

        ConstantChunkSizeFunction(int value) {
            this.value = value;
        }

        @Override
        public int call(Object[] args)
        {
            return value;
        }
    }

    private static class ParamBasedChunkSizeFunction implements ChunkSizeFunction
    {
        private final int index;

        ParamBasedChunkSizeFunction(int index) {
            this.index = index;
        }

        @Override
        public int call(Object[] args)
        {
            return (Integer)args[index];
        }
    }

    private static boolean returnTypeIsValid(Class<?> type) {
        if (type.equals(Void.TYPE) || type.isArray() && (type.getComponentType().equals(Integer.TYPE) || type.getComponentType().equals(Long.TYPE))) {
            return true;
        }

        return false;
    }

    private static String invalidReturnTypeMessage(ResolvedMethod method) {
        return method.getDeclaringType() + "." + method +
                " method is annotated with @SqlBatch so should return void or int[] but is returning: " +
                method.getReturnType();
    }
}
