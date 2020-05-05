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

import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.skife.jdbi.v2.ClasspathStatementLocator;
import org.skife.jdbi.v2.SQLStatement;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizer;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizerFactory;
import org.skife.jdbi.v2.sqlobject.SqlStatementCustomizingAnnotation;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator.LocatorFactory;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocatorImpl;

/**
 * Used to set attributes on the StatementContext for the statement generated for this method.
 * These values will be available to other customizers, such as the statement locator or rewriter.
 */
@SqlStatementCustomizingAnnotation(Define.Factory.class)
@Target(ElementType.PARAMETER)
@Retention(RetentionPolicy.RUNTIME)
public @interface Define
{
    /**
     * The key for the attribute to set. The value will be the value passed to the annotated argument
     */
    String value();

    class Factory implements SqlStatementCustomizerFactory
    {
        private static final Set<Class<?>> ALLOWED_ARG_TYPES = new HashSet<Class<?>>(
            Arrays.asList(
                Boolean.class,
                Character.class,
                Byte.class,
                Short.class,
                Integer.class,
                Long.class,
                Float.class,
                Double.class,
                Void.class,
                String.class
            )
        );

        @Override
        public SqlStatementCustomizer createForType(Annotation annotation, Class sqlObjectType)
        {
            throw new UnsupportedOperationException("Not allowed on Type");
        }

        @Override
        public SqlStatementCustomizer createForMethod(Annotation annotation, Class sqlObjectType, Method method)
        {
            throw new UnsupportedOperationException("Not allowed on Method");
        }

        @Override
        public SqlStatementCustomizer createForParameter(Annotation annotation, final Class sqlObjectType, Method method, final Object arg)
        {
            validateArgType(arg);

            final Object validatedArg;

            if (arg instanceof String) {
                String argString = (String) arg;
                validateArgString(argString);
                validatedArg = '`' + argString + "`";
            } else {
                validatedArg = arg;
            }

            Define d = (Define) annotation;
            final String key = d.value();
            return new SqlStatementCustomizer()
            {
                @Override
                public void apply(SQLStatement q) throws SQLException
                {
                    q.define(key, validatedArg);
                    if (q.getStatementLocator() instanceof ClasspathStatementLocator) {
                        new LocatorFactory().createForType(UseStringTemplate3StatementLocatorImpl.defaultInstance(), sqlObjectType).apply(q);
                    }
                }
            };
        }

        private static void validateArgType(Object arg) {
            if (arg == null) {
                return;
            }

            Class<?> argType = arg.getClass();
            if (argType.isEnum() || argType.isPrimitive()) {
                return;
            }

            if (!ALLOWED_ARG_TYPES.contains(argType)) {
                throw new IllegalArgumentException("Disallowed @Define argument type " + arg);
            }
        }

        /**
         * Requires ascii letter or digit
         */
        private static void validateArgString(String argString) {
            for (char c : argString.toCharArray()) {
                if (c < 32 || c > 127 || !Character.isLetterOrDigit(c)) {
                    throw new IllegalArgumentException(
                        "Disallowed character " + c + " in @Define argument " + argString
                    );
                }
            }
        }
    }
}
