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
package org.skife.jdbi.v2.unstable;

import org.skife.jdbi.v2.ClasspathStatementLocator;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.sqlobject.Binder;
import org.skife.jdbi.v2.sqlobject.BinderFactory;
import org.skife.jdbi.v2.sqlobject.BindingAnnotation;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizer;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizerFactory;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizingAnnotation;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator.LocatorFactory;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocatorImpl;

import java.lang.annotation.Annotation;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.reflect.Array;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;

/**
 * Binds an Iterable or array/varargs of any type to a placeholder as a comma-separated list (e.g. for WHERE X IN (...) query statements).
 *
 * Throws IllegalArgumentException if the argument is not an array or Iterable. How null and empty collections are handled can be configured with onEmpty:EmptyHandling - throws IllegalArgumentException by default.
 *
 * Don't forget to add @UseStringTemplate3StatementLocator to your query class and use the correct placeholder format:
 *  {@literal @}SqlQuery("SELECT * FROM THINGS WHERE ID IN ({@literal <}ids{@literal >})")
 *  abstract Object[] foo({@literal @}BindIn("ids") int[] ids);
 *  ids = [1, 2, 3] -> SELECT * FROM THINGS WHERE ID IN (1,2,3)
 */
@Retention(RetentionPolicy.RUNTIME)
@SqlStatementCustomizingAnnotation(BindIn.CustomizerFactory.class)
@BindingAnnotation(BindIn.BindingFactory.class)
public @interface BindIn
{
    /**
     * placeholder in your query to be replaced with comma-separated list
     */
    String value();

    /**
     * what to do when the argument is null or empty
     */
    EmptyHandling onEmpty() default EmptyHandling.THROW;

    final class CustomizerFactory implements SqlStatementCustomizerFactory
    {
        @Override
        public SqlStatementCustomizer createForMethod(final Annotation annotation, final Class sqlObjectType, final Method method)
        {
            throw new UnsupportedOperationException("Not supported on method");
        }

        @Override
        public SqlStatementCustomizer createForType(final Annotation annotation, final Class sqlObjectType)
        {
            throw new UnsupportedOperationException("Not supported on type");
        }

        @Override
        public SqlStatementCustomizer createForParameter(final Annotation annotation, final Class sqlObjectType, final Method method, final Object arg)
        {
            final BindIn bindIn = (BindIn) annotation;

            final int size;
            if (arg == null)
            {
                switch (bindIn.onEmpty())
                {
                    case VOID:
                        // skip argument iteration altogether to output nothing at all
                        size = 0;
                        break;
                    case NULL:
                        // output exactly 1 value: null
                        size = 1;
                        break;
                    case THROW:
                        throw new IllegalArgumentException("argument is null; null was explicitly forbidden on this instance of BindIn");
                    default:
                        throw new IllegalStateException(EmptyHandling.valueNotHandledMessage);
                }
            } else
            {
                size = Util.size(arg);
            }

            final String key = bindIn.value();

            // generate and concat placeholders
            final StringBuilder names = new StringBuilder();
            for (int i = 0; i < size; i++)
            {
                if (i > 0)
                {
                    names.append(",");
                }
                names.append(":__").append(key).append("_").append(i);
            }

            if (size == 0)
            {
                switch (bindIn.onEmpty())
                {
                    case VOID:
                        // output nothing - taken care of with size = 0
                        break;
                    case NULL:
                        names.append((String) null); // force 'null' placeholder in IN clause
                        break;
                    case THROW:
                        throw new IllegalArgumentException("argument is empty; emptiness was explicitly forbidden on this instance of BindIn");
                    default:
                        throw new IllegalStateException(EmptyHandling.valueNotHandledMessage);
                }
            }

            final String ns = names.toString();

            return new SqlStatementCustomizer()
            {
                @Override
                public void apply(final SQLStatement q) throws SQLException
                {
                    q.define(key, ns);
                    if (q.getStatementLocator() instanceof ClasspathStatementLocator) {
                        new LocatorFactory().createForType(UseStringTemplate3StatementLocatorImpl.defaultInstance(), sqlObjectType).apply(q);
                    }
                }
            };
        }
    }

    class BindingFactory implements BinderFactory<BindIn>
    {
        @Override
        public Binder build(final BindIn bindIn)
        {
            final String key = bindIn.value();

            return new Binder<Annotation, Object>()
            {
                @Override
                public void bind(final SQLStatement q, final Annotation bind, final Object arg)
                {
                    if (arg == null || Util.size(arg) == 0)
                    {
                        switch (bindIn.onEmpty())
                        {
                            case VOID:
                                // output nothing, end now
                                break;
                            case NULL:
                                // output null
                                q.bind("__" + key + "_0", (String) null);
                                break;
                            case THROW:
                                final Exception inner = new IllegalArgumentException("argument is null; null was explicitly forbidden on this instance of BindIn");
                                throw new IllegalStateException("Illegal argument value was caught too late. Please report this to the jdbi developers.", inner);
                            default:
                                throw new IllegalStateException(EmptyHandling.valueNotHandledMessage);
                        }
                    } else
                    {
                        // replace placeholders with actual values
                        final Iterator it = Util.toIterator(arg);
                        for (int i = 0; it.hasNext(); i++)
                        {
                            q.bind("__" + key + "_" + i, it.next());
                        }
                    }
                }
            };
        }
    }

    final class Util
    {
        private Util()
        {
        }

        static Iterator toIterator(final Object obj)
        {
            if (obj == null)
            {
                throw new IllegalArgumentException("cannot make iterator of null");
            }

            if (obj instanceof Iterable)
            {
                return ((Iterable) obj).iterator();
            }

            if (obj.getClass().isArray())
            {
                if (obj instanceof Object[])
                {
                    return Arrays.asList((Object[]) obj).iterator();
                } else
                {
                    return new ReflectionArrayIterator(obj);
                }
            }

            throw new IllegalArgumentException(getTypeWarning(obj.getClass()));
        }

        static int size(final Object obj)
        {
            if (obj == null)
            {
                throw new IllegalArgumentException("cannot get size of null");
            }

            if (obj instanceof Collection)
            {
                return ((Collection) obj).size();
            }

            if (obj instanceof Iterable)
            {
                final Iterable<?> iterable = (Iterable) obj;

                int size = 0;
                for (final Object x : iterable)
                {
                    size++;
                }

                return size;
            }

            if (obj.getClass().isArray())
            {
                return Array.getLength(obj);
            }

            throw new IllegalArgumentException(getTypeWarning(obj.getClass()));
        }

        private static String getTypeWarning(final Class type)
        {
            return "argument must be one of the following: Iterable, or an array/varargs (primitive or complex type); was " + type.getName() + " instead";
        }
    }

    /**
     * describes what needs to be done if the passed argument is null or empty
     */
    enum EmptyHandling
    {
        /**
         * output "" (without quotes, i.e. nothing)
         *
         * select * from things where x in ()
         */
        VOID,
        /**
         * output "null" (without quotes, as keyword), useful e.g. in postgresql where "in ()" is invalid syntax
         *
         * select * from things where x in (null)
         */
        NULL,
        /**
         * throw IllegalArgumentException
         */
        THROW;

        static final String valueNotHandledMessage = "EmptyHandling type on BindIn not handled. Please report this to the jdbi developers.";
    }
}
