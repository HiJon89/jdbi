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

import org.h2.jdbcx.JdbcDataSource;
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
import java.util.Iterator;
import java.util.List;
import java.util.UUID;

import static org.skife.jdbi.v2.unstable.BindIn.EmptyHandling.THROW;
import static org.skife.jdbi.v2.unstable.BindIn.EmptyHandling.VOID;

public class BindInTest
{
    private static Handle handle;

    @BeforeClass
    public static void init()
    {
        final JdbcDataSource ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:mem:" + UUID.randomUUID());
        final DBI dbi = new DBI(ds);
        dbi.registerMapper(new SomethingMapper());
        handle = dbi.open();

        handle.execute("create table something (id int primary key, name varchar(100))");
        handle.execute("insert into something(id, name) values(1, '1')");
        handle.execute("insert into something(id, name) values(2, '2')");

        // "control group" element that should *not* be returned by the queries
        handle.execute("insert into something(id, name) values(3, '3')");
    }

    @AfterClass
    public static void exit()
    {
        handle.execute("drop table something");
        handle.close();
    }

    //

    @Test
    public void testSomethingByVarargsHandleDefaultWithVarargs()
    {
        final SomethingByVarargsHandleDefault s = handle.attach(SomethingByVarargsHandleDefault.class);

        final List<Something> out = s.get(1, 2);

        Assert.assertEquals(2, out.size());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByVarargsHandleDefault
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn("ids") int... ids);
    }

    //

    @Test
    public void testSomethingByArrayHandleVoidWithArray()
    {
        final SomethingByArrayHandleVoid s = handle.attach(SomethingByArrayHandleVoid.class);

        final List<Something> out = s.get(new int[]{1, 2});

        Assert.assertEquals(2, out.size());
    }

    @Test
    public void testSomethingByArrayHandleVoidWithEmptyArray()
    {
        final SomethingByArrayHandleVoid s = handle.attach(SomethingByArrayHandleVoid.class);

        final List<Something> out = s.get(new int[]{});

        Assert.assertEquals(0, out.size());
    }

    @Test
    public void testSomethingByArrayHandleVoidWithNull()
    {
        final SomethingByArrayHandleVoid s = handle.attach(SomethingByArrayHandleVoid.class);

        final List<Something> out = s.get(null);

        Assert.assertEquals(0, out.size());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByArrayHandleVoid
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn(value = "ids", onEmpty = VOID) int[] ids);
    }

    //

    @Test(expected = IllegalArgumentException.class)
    public void testSomethingByArrayHandleThrowWithNull()
    {
        final SomethingByArrayHandleThrow s = handle.attach(SomethingByArrayHandleThrow.class);

        s.get(null);
    }

    @Test(expected = IllegalArgumentException.class)
    public void testSomethingByArrayHandleThrowWithEmptyArray()
    {
        final SomethingByArrayHandleThrow s = handle.attach(SomethingByArrayHandleThrow.class);

        s.get(new int[]{});
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByArrayHandleThrow
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn(value = "ids", onEmpty = THROW) int[] ids);
    }

    //

    @Test
    public void testSomethingByIterableHandleDefaultWithIterable()
    {
        final SomethingByIterableHandleDefault s = handle.attach(SomethingByIterableHandleDefault.class);

        final List<Something> out = s.get(new Iterable<Integer>()
        {
            @Override
            public Iterator<Integer> iterator()
            {
                final List<Integer> out = new ArrayList<Integer>();
                out.add(1);
                out.add(2);
                return out.iterator();
            }
        });

        Assert.assertEquals(2, out.size());
    }

    @Test
    public void testSomethingByIterableHandleDefaultWithEmptyIterable()
    {
        final SomethingByIterableHandleDefault s = handle.attach(SomethingByIterableHandleDefault.class);

        final List<Something> out = s.get(new ArrayList<Integer>());

        Assert.assertEquals(0, out.size());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByIterableHandleDefault
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn(value = "ids", onEmpty = VOID) Iterable<Integer> ids);
    }

    //

    @Test(expected = IllegalArgumentException.class)
    public void testSomethingByIterableHandleThrowWithEmptyIterable()
    {
        final SomethingByIterableHandleThrow s = handle.attach(SomethingByIterableHandleThrow.class);

        s.get(new ArrayList<Integer>());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByIterableHandleThrow
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn(value = "ids", onEmpty = THROW) Iterable<Integer> ids);
    }

    //

    @Test(expected = IllegalArgumentException.class)
    public void testSomethingByIteratorHandleDefault()
    {
        final SomethingByIteratorHandleDefault s = handle.attach(SomethingByIteratorHandleDefault.class);

        final List<Integer> in = new ArrayList<Integer>();
        in.add(1);
        in.add(2);

        s.get(in.iterator());
    }

    @UseStringTemplate3StatementLocator
    private interface SomethingByIteratorHandleDefault
    {
        @SqlQuery("select id, name from something where id in (<ids>)")
        List<Something> get(@BindIn("ids") Iterator<Integer> ids);
    }
}