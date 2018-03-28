package hbase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.alibaba.fastjson.JSONObject;

/**
 * 
 * Title: HBaseUtil Description: HBase������ Version:1.0.0
 * 
 * @author pancm
 * @date 2017��12��6��
 */
public class HBaseUtil {
	private static Configuration conf = null;
	private static Connection con = null;
	private static Admin admin = null;

	static {
		// ��������ļ�����
		conf = HBaseConfiguration.create();
		// �������ò���
		conf.set("hbase.zookeeper.quorum", "192.168.58.155,192.168.58.156,192.168.58.157");
		conf.set("hbase.zookeeper.property.clientPort", "2181");
		//conf.set("zookeeper.znode.parent", "/hbase");
	}

	public synchronized static Connection getConnection() {
		try {
			if (null == con || con.isClosed()) {
				// ������Ӷ���
				con = ConnectionFactory.createConnection(conf);
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return con;
	}

	public static void close() {
		try {
			if (admin != null) {
				admin.close();
			}
			if (con != null) {
				con.close();
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void creatTable(String tableName, String[] columnFamily) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == columnFamily || columnFamily.length == 0) {
			return;
		}
		TableName tn = TableName.valueOf(tableName);
		try {
			admin = getConnection().getAdmin();
			if (admin.tableExists(tn)) {
				System.out.println(tableName + " table exists,delete it.");
				admin.disableTable(tn);
				admin.deleteTable(tn);
				System.out.println("deleted.....");
			}
			HTableDescriptor htd = new HTableDescriptor(tn);
			for (String str : columnFamily) {
				HColumnDescriptor hcd = new HColumnDescriptor(str);
				htd.addFamily(hcd);
			}
			admin.createTable(htd);
			System.out.println(tableName + " created!");
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void insert(String tableName, String rowKey, String family, String qualifier, String value) {
		Table t = null;
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			Put put = new Put(Bytes.toBytes(rowKey));
			put.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
			t.put(put);
		} catch (IOException e) {
			System.out.println(tableName + " update fail!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void insertBatch(String tableName, List<?> list) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == list || list.size() == 0) {
			return;
		}
		Table t = null;
		Put put = null;
		JSONObject json = null;
		List<Put> puts = new ArrayList<Put>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			for (int i = 0, j = list.size(); i < j; i++) {
				json = (JSONObject) list.get(i);
				put = new Put(Bytes.toBytes(json.getString("rowKey")));
				put.addColumn(Bytes.toBytes(json.getString("family")), Bytes.toBytes(json.getString("qualifier")),
						Bytes.toBytes(json.getString("value")));
				puts.add(put);
			}
			t.put(puts);
			System.out.println(tableName + " update done!");
		} catch (IOException e) {
			System.out.println(tableName + " update failed!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void delete(String tableName, String rowKey) {
		delete(tableName, rowKey, "", "");
	}

	public static void delete(String tableName, String rowKey, String family) {
		delete(tableName, rowKey, family, "");
	}

	public static void delete(String tableName, String rowKey, String family, String qualifier) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		if (null == rowKey || rowKey.length() == 0) {
			return;
		}
		Table t = null;
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			Delete del = new Delete(Bytes.toBytes(rowKey));
			if (null != family && family.length() > 0) {
				if (null != qualifier && qualifier.length() > 0) {
					del.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				} else {
					del.addFamily(Bytes.toBytes(family));
				}
			}
			t.delete(del);
		} catch (IOException e) {
			System.out.println("!");
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void select(String tableName) {
		if (null == tableName || tableName.length() == 0) {
			return;
		}
		Table t = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			// ��ȡ����
			Scan scan = new Scan();
			ResultScanner rs = t.getScanner(scan);
			if (null == rs) {
				return;
			}
			for (Result result : rs) {
				List<Cell> cs = result.listCells();
				if (null == cs || cs.size() == 0) {
					continue;
				}
				for (Cell cell : cs) {
					Map<String, Object> map = new HashMap<String, Object>();
					map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// ȡ�н�
					map.put("timestamp", cell.getTimestamp());// ȡ��ʱ���
					map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// ȡ������
					map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// ȡ����
					map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// ȡ��ֵ
					list.add(map);
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}

	public static void select(String tableName, String rowKey) {
		select(tableName, rowKey, "", "");
	}

	public static void select(String tableName, String rowKey, String family) {
		select(tableName, rowKey, family, "");
	}

	public static void select(String tableName, String rowKey, String family, String qualifier) {
		Table t = null;
		List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
		try {
			t = getConnection().getTable(TableName.valueOf(tableName));
			Get get = new Get(Bytes.toBytes(rowKey));
			if (null != family && family.length() > 0) {
				if (null != qualifier && qualifier.length() > 0) {
					get.addColumn(Bytes.toBytes(family), Bytes.toBytes(qualifier));
				} else {
					get.addFamily(Bytes.toBytes(family));
				}
			}
			Result r = t.get(get);
			List<Cell> cs = r.listCells();
			if (null == cs || cs.size() == 0) {
				return;
			}
			for (Cell cell : cs) {
				Map<String, Object> map = new HashMap<String, Object>();
				map.put("rowKey", Bytes.toString(CellUtil.cloneRow(cell)));// ȡ�н�
				map.put("timestamp", cell.getTimestamp());// ȡ��ʱ���
				map.put("family", Bytes.toString(CellUtil.cloneFamily(cell)));// ȡ������
				map.put("qualifier", Bytes.toString(CellUtil.cloneQualifier(cell)));// ȡ����
				map.put("value", Bytes.toString(CellUtil.cloneValue(cell)));// ȡ��ֵ
				list.add(map);
			}
		} catch (IOException e) {
			e.printStackTrace();
		} finally {
			close();
		}
	}
}