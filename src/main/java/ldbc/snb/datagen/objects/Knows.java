/*
 * Copyright (c) 2013 LDBC
 * Linked Data Benchmark Council (http://ldbc.eu)
 *
 * This file is part of ldbc_socialnet_dbgen.
 *
 * ldbc_socialnet_dbgen is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 * 
 * ldbc_socialnet_dbgen is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 * 
 * You should have received a copy of the GNU General Public License
 * along with ldbc_socialnet_dbgen.  If not, see <http://www.gnu.org/licenses/>.
 *
 * Copyright (C) 2011 OpenLink Software <bdsmt@openlinksw.com>
 * All Rights Reserved.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation;  only Version 2 of the License dated
 * June 1991.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program; if not, write to the Free Software
 * Foundation, Inc., 51 Franklin Street, Fifth Floor, Boston, MA  02110-1301, USA.
 */
package ldbc.snb.datagen.objects;

import ldbc.snb.datagen.objects.IP;
import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;



public class Knows implements Writable, Comparable<Knows> {

	Person.PersonSummary from_ = null;
	Person.PersonSummary to_= null;
	long creationDate_;

	public Knows() {
		from_ = new Person.PersonSummary();
		to_ = new Person.PersonSummary();
	}

    public Knows(Knows k) {
	    from_ = new Person.PersonSummary(k.from());
	    to_ = new Person.PersonSummary(k.to());
	    creationDate_ = k.creationDate();
    }

    public Knows(Person from, Person to, long creationDate ){
	    from_ = new Person.PersonSummary(from);
	    to_ = new Person.PersonSummary(to);
	    creationDate_ = creationDate;
    }

    public Person.PersonSummary from() {
	    return from_;
    }

    public void from( Person.PersonSummary from ) {
	    from_.copy(from);
    }

    public Person.PersonSummary to ( ) {
	    return to_;
    }

    public void to( Person.PersonSummary to ) {
	    to_.copy(to);
    }

    public long creationDate() {
	    return creationDate_;
    }

    public void creationDate ( long creationDate ) {
	    creationDate_ = creationDate;
    }


    public void readFields(DataInput arg0) throws IOException {
        from_.readFields(arg0);
        to_.readFields(arg0);
        creationDate_ = arg0.readLong();
    }

    public void write(DataOutput arg0) throws IOException {
	    from_.write(arg0);
	    to_.write(arg0);
	    arg0.writeLong(creationDate_);
    }

    public int compareTo(Knows k) {
        long res = from_.creationDate() - k.from().creationDate();
        if(res==0)  res = to_.creationDate() - k.to().creationDate();
        return (int)res;
    }
}
