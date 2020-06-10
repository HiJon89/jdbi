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
package org.skife.jdbi.v2.sqlobject.customizers;

import org.skife.jdbi.v2.Query;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizer;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizerFactory;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizingAnnotation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.sql.SQLException;

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.METHOD, ElementType.PARAMETER})
@SqlStatementCustomizingAnnotation(FetchSize.Factory.class)
public @interface FetchSize
{
    Logger LOG = LoggerFactory.getLogger(FetchSize.class);

    String VITESS_STREAMING_WARNING = "Warning: streaming reads to Vitess do not " +
        "inherit transactional context. You will not be able to read your writes.";

    int value() default 0;

    class Factory implements SqlStatementCustomizerFactory
    {

        @Override
        public SqlStatementCustomizer createForMethod(Annotation annotation, Class sqlObjectType, Method method)
        {
            final FetchSize fs = (FetchSize) annotation;
            return new SqlStatementCustomizer()
            {
                @Override
                public void apply(SQLStatement q) throws SQLException
                {
                    assert q instanceof Query;
                    logIfStreamingInTransaction(q, fs);
                    ((Query) q).setFetchSize(fs.value());
                }
            };
        }

        @Override
        public SqlStatementCustomizer createForType(Annotation annotation, Class sqlObjectType)
        {
            final FetchSize fs = (FetchSize) annotation;
            return new SqlStatementCustomizer()
            {
                @Override
                public void apply(SQLStatement q) throws SQLException
                {
                    assert q instanceof Query;
                    logIfStreamingInTransaction(q, fs);
                    ((Query) q).setFetchSize(fs.value());
                }
            };
        }

        @Override
        public SqlStatementCustomizer createForParameter(Annotation annotation, Class sqlObjectType, Method method, Object arg)
        {
            final Integer va = (Integer) arg;
            return new SqlStatementCustomizer()
            {
                @Override
                public void apply(SQLStatement q) throws SQLException
                {
                    assert q instanceof Query;
                    ((Query) q).setFetchSize(va);
                }
            };
        }

        private void logIfStreamingInTransaction(SQLStatement q, FetchSize fetchSize) throws SQLException {
            if (fetchSize.value() > 0 && !q.getContext().getConnection().getAutoCommit()) {
                LOG.error(VITESS_STREAMING_WARNING);
            }
        }
    }
}
