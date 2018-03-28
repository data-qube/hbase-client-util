package hbase;

public class HbaseTest {

	public static void main(String[] args) {
		test();
		test1();
	}

	private static void test() {
		String tableName1 = "t_student", tableName2 = "t_student_info";
		String[] columnFamily1 = { "st1", "st2" };
		String[] columnFamily2 = { "stf1", "stf2" };

		HBaseUtil.creatTable(tableName1, columnFamily1);
		HBaseUtil.creatTable(tableName2, columnFamily2);

	}
	
	private static void test1() {
		String tableName1 = "t_student", tableName2 = "t_student_info";
		String[] columnFamily1 = { "st1", "st2" };
		String[] columnFamily2 = { "stf1", "stf2" };

		HBaseUtil.insert(tableName1, "1001", columnFamily1[0], "name", "zhangsan");
		HBaseUtil.insert(tableName1, "1002", columnFamily1[0], "name", "lisi");
		HBaseUtil.insert(tableName1, "1001", columnFamily1[1], "age", "18");
		HBaseUtil.insert(tableName1, "1002", columnFamily1[1], "age", "20");

		HBaseUtil.insert(tableName2, "1001", columnFamily2[0], "phone", "123456");
		HBaseUtil.insert(tableName2, "1002", columnFamily2[0], "phone", "234567");
		HBaseUtil.insert(tableName2, "1001", columnFamily2[1], "mail", "123@163.com");
		HBaseUtil.insert(tableName2, "1002", columnFamily2[1], "mail", "234@163.com");

	}
	
	private static void test2() {
		String tableName1 = "t_student", tableName2 = "t_student_info";
		String[] columnFamily1 = { "st1", "st2" };
		String[] columnFamily2 = { "stf1", "stf2" };

		HBaseUtil.select(tableName1);
		HBaseUtil.select(tableName1,"1001");
		HBaseUtil.select(tableName2,"1002",columnFamily2[0]);
		HBaseUtil.select(tableName2,"1002",columnFamily2[1],"mail");


	}
	
	private static void test3() {
		String tableName1 = "t_student", tableName2 = "t_student_info";
		String[] columnFamily1 = { "st1", "st2" };
		String[] columnFamily2 = { "stf1", "stf2" };

		HBaseUtil.select(tableName1,"1002");
		HBaseUtil.delete(tableName1, "1002",columnFamily1[0]);
		HBaseUtil.select(tableName1,"1002");
	}
	
	private static void test4()
	{
		long startTime = System.currentTimeMillis();
		for(int i =0 ;i < 10000; i++) {
			HBaseUtil.insert("t_student", String.valueOf(i), "st1", "price", String.valueOf(i));
		}
		long endTime = System.currentTimeMillis();
		System.out.println("time taken" + (endTime - startTime) + "ms");
	}
	
	private static void test5()
	{
		
	}
}
