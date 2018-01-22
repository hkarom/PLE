package bigdata.heights;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.util.Tool;

import scala.Tuple2;

public class HeightsLvl {
	
	public static class HBaseProg extends Configured implements Tool {
		private static final byte[] LOC = Bytes.toBytes("loc");
		private static final byte[] MEASURE = Bytes.toBytes("measure");
		private static List<Tuple2<String, Integer>> values = new ArrayList<>();
		

		
		private static final byte[] TABLE_NAME = Bytes.toBytes("HeightsLVL");

		public static void createOrOverwrite(Admin admin, HTableDescriptor table) throws IOException {
			if (admin.tableExists(table.getTableName())) {
				admin.disableTable(table.getTableName());
				admin.deleteTable(table.getTableName());
			}
			admin.createTable(table);
		}

		public static void createTable(Connection connect) {
			try {
				final Admin admin = connect.getAdmin(); 
				HTableDescriptor tableDescriptor = new HTableDescriptor(TableName.valueOf(TABLE_NAME));

				tableDescriptor.addFamily(new HColumnDescriptor(LOC));
				tableDescriptor.addFamily(new HColumnDescriptor(MEASURE));

				createOrOverwrite(admin, tableDescriptor);
				admin.close();
			} catch (Exception e) {
				e.printStackTrace();
				System.exit(-1);
			}
		}
		
		public static void send(List<Tuple2<String, Integer>> val) {
			values = val;
		}

		public int run(String[] args) throws IOException {
			Connection connection = ConnectionFactory.createConnection(getConf());
			createTable(connection);
			Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
			for(int i =0; i<values.size(); i++) {
				Put put = new Put(Bytes.toBytes(i));
				String coord[] = values.get(i)._1().split(",");
				put.addColumn(Bytes.toBytes("loc"),Bytes.toBytes('x'), Bytes.toBytes(coord[0]));
				put.addColumn(Bytes.toBytes("loc"),Bytes.toBytes('y'), Bytes.toBytes(coord[1]));
				put.addColumn(Bytes.toBytes("measure"),Bytes.toBytes("height"), Bytes.toBytes(values.get(i)._2()));
				put.addColumn(Bytes.toBytes("measure"),Bytes.toBytes("scale"), Bytes.toBytes(1));
			table.put(put);
			}
			return 0;
		}

	}

}
