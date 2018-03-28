package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

public class Main {

    public static void main(String[] args) throws Exception {

        Configuration conf = HBaseConfiguration.create();
        // zookeeper configure
//        conf.set("hbase.zookeeper.quorum", "192.168.130.141,192.168.130.142,192.168.130.143");
        conf.set("hbase.zookeeper.quorum", "192.168.58.155,192.168.58.156,192.168.58.157");
         conf.set("hbase.zookeeper.property.clientPort", "2181");
        conf.set("zookeeper.znode.parent", "/hbase");
        Connection con = ConnectionFactory.createConnection(conf);
        HTable t = (HTable) con.getTable(TableName.valueOf("my_test"));
//		 insert one by one
        try {
            long startTime = System.currentTimeMillis(); //start
            for (int i = 0; i < 10; i++) {
                Put put = new Put(Bytes.toBytes(i));
                for (int j = 0; j < 50; j++) {
                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f" + j),
                            Bytes.toBytes("AAAAAAAAAAAAAAAAAAAA"));
                }
                t.put(put);
            }
            long endTime = System.currentTimeMillis(); //end

            System.out.println("execution time: " + (endTime - startTime) + "ms");

        } catch (Exception e) {
            System.out.println("my_test" + " update failed!");
            e.printStackTrace();
        } finally {
        }

        // batch insert
        // try {
        // long startTime=System.currentTimeMillis(); //start time
        // List<Put> puts = new ArrayList<Put>();
        // for(int i = 0; i< 10000;i++) {
        // Put put = new Put(Bytes.toBytes(i));
        // for(int j =0 ;j <50 ;j++) {
        // put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f"+j),
        // Bytes.toBytes("AAAAAAAAAAAAAAAAAAAA"));
        // }
        // puts.add(put);
        // }
        // t.put(puts);
        // long endTime=System.currentTimeMillis(); //end time
        //
        // System.out.println("execution time： "+(endTime-startTime)+"ms");
        //
        // } catch (IOException e) {
        // System.out.println("my_test" + " update failed!");
        // e.printStackTrace();
        // } finally {
        // }

        // batch insert
        // try {
        // t.setAutoFlushTo(false);
        // t.setWriteBufferSize(5 * 1024 * 1024);
        // long startTime=System.currentTimeMillis(); //start time
        // List<Put> puts = new ArrayList<Put>();
        // for(int i = 0; i< 10000;i++) {
        // Put put = new Put(Bytes.toBytes(i));
        // for(int j =0 ;j <50 ;j++) {
        // put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f"+j),
        // Bytes.toBytes("AAAAAAAAAAAAAAAAAAAA"));
        // put.setWriteToWAL(false);
        // }
        // puts.add(put);
        // }
        // t.put(puts);
        // t.flushCommits();
        // long endTime=System.currentTimeMillis(); //get end time
        //
        // System.out.println("execution time： "+(endTime-startTime)+"ms");
        //
        // } catch (IOException e) {
        // System.out.println("my_test" + " update failed!");
        // e.printStackTrace();
        // } finally {
        // }

//        System.out.println("---------MultThreadInsert test starts----------");
//        long start = System.currentTimeMillis();
//        int threadNumber = 10;
//        Thread[] threads = new Thread[threadNumber];
//        for (int i = 0; i < threads.length; i++) {
//            threads[i] = new InsertThread(con, String.valueOf(i));
//            threads[i].start();
//        }
//        for (int j = 0; j < threads.length; j++) {
//            (threads[j]).join();
//        }
//        long stop = System.currentTimeMillis();
//
//        System.out.println("MultThreadInsert：" + threadNumber * 10000 + "time taken:" + (stop - start) * 1.0 / 1000 + "s");
//        System.out.println("---------MultThreadInsert test end----------");
    }
}

//class InsertThread extends Thread {
//    private Connection conn;
//    //private HTable hTable;
//    private String flag;
//
//    public InsertThread(Connection conn, String flag) {
//        this.conn = conn;
//        this.flag = flag;
//    }
//
//    public void insert() {
//        try {
//
//            HTable hTable = (HTable) conn.getTable(TableName.valueOf("my_test"));
//            hTable.setAutoFlushTo(false);
//            hTable.setWriteBufferSize(5 * 1024 * 1024);
//
//            List<Put> puts = new ArrayList<Put>();
//            for (int i = 0; i < 10000; i++) {
//                Put put = new Put(Bytes.toBytes(flag + i));
//                for (int j = 0; j < 50; j++) {
//                    put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("f" + j), Bytes.toBytes("AAAAAAAAAAAAAAAAAAAA"));
//                    put.setWriteToWAL(false);
//                }
//                puts.add(put);
//            }
//            long startTime = System.currentTimeMillis(); // start time
//            hTable.put(puts);
//            hTable.flushCommits();
//            long endTime = System.currentTimeMillis(); // end time
//
//            System.out.println(Thread.currentThread().getId() + "execution time: " + (endTime - startTime) + "ms");
//
//        } catch (IOException e) {
//            System.out.println("my_test" + " update failed!");
//            e.printStackTrace();
//        } finally {
//        }
//    }
//
//    public void run() {
//        try {
//            insert();
//        } catch (Exception e) {
//            e.printStackTrace();
//        } finally {
//            System.gc();
//        }
//    }
//}
