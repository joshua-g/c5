/*
 * Copyright (C) 2013  Ohm Data
 *
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU Affero General Public License as
 *  published by the Free Software Foundation, either version 3 of the
 *  License, or (at your option) any later version.
 *
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU Affero General Public License for more details.
 *
 *  You should have received a copy of the GNU Affero General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *  This file incorporates work covered by the following copyright and
 *  permission notice:
 */
package c5db.client;


import com.google.protobuf.ByteString;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class TestMultiUtil {

  ByteString tableName = ByteString.copyFrom(Bytes.toBytes("tableName"));
  OhmTable table;
  byte[] row = Bytes.toBytes("multiRow");
  byte[] cf = Bytes.toBytes("cf");
  byte[] cq = Bytes.toBytes("cq");
  byte[] value = Bytes.toBytes("value");

  public TestMultiUtil() throws IOException, InterruptedException {
    table = new OhmTable(tableName);
  }

  public static void main(String[] args) throws IOException, InterruptedException {
    TestMultiUtil testingUtil = new TestMultiUtil();
    try {
      testingUtil.testMultiPut();
      testingUtil.testScan();
      testingUtil.testMultiDelete();
      testingUtil.testScan();
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  private void testMultiPut() throws IOException {
    List<Put> puts = new ArrayList<>();
    puts.add(new Put(row).add(cf, cq, value));
    puts.add(new Put(Bytes.add(row, row)).add(cf, cq, value));
    table.put(puts);
  }


  public void testScan() throws IOException {
    Scan scan = new Scan(row);
    scan.addColumn(cf, cq);
    ResultScanner resultScanner = table.getScanner(scan);
    Result r = resultScanner.next();
    r = resultScanner.next();
    r.toString();
  }


  private void testMultiDelete() throws IOException {
    List<Delete> deletes = new ArrayList<>();
    deletes.add(new Delete(row));
    deletes.add(new Delete(Bytes.add(row, row)));
    table.delete(deletes);
  }

}