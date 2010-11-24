/*
 * (C) Copyright 2010 Nuxeo SA (http://nuxeo.com/) and contributors.
 *
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the GNU Lesser General Public License
 * (LGPL) version 2.1 which accompanies this distribution, and is available at
 * http://www.gnu.org/licenses/lgpl.html
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * Contributors:
 *     Florent Guillaume
 */
package org.nuxeo.ecm.core.storage.sql;

import junit.framework.TestCase;

public class TestSQLBinaryManagerParseSize extends TestCase {

    SQLBinaryManager bm = new SQLBinaryManager();

    public long parse(String string) {
        return bm.parseSizeInBytes(string);
    }

    public void testParseSize() throws Exception {
        assertEquals(0, parse("0"));
        assertEquals(1, parse("1"));
        assertEquals(1, parse("1B"));
        assertEquals(2 * 1024, parse("2K"));
        assertEquals(2 * 1024, parse("2kB"));
        assertEquals(3 * 1024 * 1024, parse("3M"));
        assertEquals(3 * 1024 * 1024, parse("3MB"));
        assertEquals(4 * 1024 * 1024 * 1024L, parse("4G"));
        assertEquals(4 * 1024 * 1024 * 1024L, parse("4Gb"));
    }
}
