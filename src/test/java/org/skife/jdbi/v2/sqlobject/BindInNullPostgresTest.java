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

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.skife.jdbi.v2.DBI;
import org.skife.jdbi.v2.Handle;
import org.skife.jdbi.v2.Something;
import org.skife.jdbi.v2.sqlobject.stringtemplate.UseStringTemplate3StatementLocator;
import org.skife.jdbi.v2.unstable.BindIn;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assume.assumeTrue;
import static org.skife.jdbi.v2.unstable.BindIn.EmptyHandling.NULL;

public class BindInNullPostgresTest {
    private static Handle handle;

    @BeforeClass
    public static void init() {
        assumeTrue(Boolean.parseBoolean(System.getenv("TRAVIS")));

        handle = new DBI("jdbc:postgresql:jdbi_test", "postgres", "").open();

        handle.execute("create table something (id int primary key, name varchar(100))");
        handle.execute("insert into something(id, name) values(1, null)");
        handle.execute("insert into something(id, name) values(2, 'bla')");
        handle.execute("insert into something(id, name) values(3, 'null')");
        handle.execute("insert into something(id, name) values(4, '')");
    }

    @AfterClass
    public static void exit() {
        if (handle != null) {
            handle.execute("drop table something");
            handle.close();
        }
    }

    @Test
    public void testSomethingByIterableHandleNullWithNull() {
        final SomethingByIterableHandleNull s = handle.attach(SomethingByIterableHandleNull.class);

        final List<Something> out = s.get(null);

        Assert.assertEquals(0, out.size());
    }

    @Test
    public void testSomethingByIterableHandleNullWithEmptyList() {
        final SomethingByIterableHandleNull s = handle.attach(SomethingByIterableHandleNull.class);

        final List<Something> out = s.get(new ArrayList<Object>());

        Assert.assertEquals(0, out.size());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByIterableHandleNull {
        @SqlQuery("select id, name from something where name in (<names>)")
        List<Something> get(@BindIn(value = "names", onEmpty = NULL) Iterable<Object> ids);
    }
}
