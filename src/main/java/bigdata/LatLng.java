package bigdata;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.MasterNotRunningException;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.ZooKeeperConnectionException;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.input.PortableDataStream;
import org.apache.spark.util.StatCounter;

import com.google.protobuf.ServiceException;

import features.Coordonnee;
import scala.Tuple2;
import scala.Tuple3;


public class LatLng implements Serializable {
	/**
	 * 
	 */
	private static final long serialVersionUID = -8080638565239238717L;
	String file;
	Coordonnee pixels[][] = new Coordonnee[64][64];
	final static int LENGTH = 1024;
	int PIX_SIZE =16;
	List<Tuple2<String, Integer>> final_results;

	LatLng(String f)  {
		this.file = f;
	}



	public void initTab() {
		int nbPoints = LENGTH / PIX_SIZE;
		int positionx = - (nbPoints / 2);
		int positiony = (nbPoints / 2);
		for(int i = 0; i < pixels.length; i ++) {
			for(int j = 0; j < pixels.length; j ++) {
				pixels[i][j] = new Coordonnee(positionx * PIX_SIZE,positiony * PIX_SIZE);
				positionx ++;
				System.out.print(pixels[i][j]);
			}
			System.out.println("\n");
			positiony--;
			positionx = - (nbPoints / 2);
		}
	}

	/*public void means() throws ServiceException, IOException {
		SparkConf conf = new SparkConf().setAppName("PLE LatLng");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd = context.textFile(file);



		JavaRDD<Tuple3<String, String,String>> rdd2 = rdd.map(line -> { 
			String[] elements = line.split(",");
			if(elements[4].equals("")) elements[4] = "-1";
			return new Tuple3<String, String, String>(elements[4], elements[5], elements[6]);
		});

		rdd2 = rdd2.filter(pop -> {
			return !pop._1().equals("-1") && !pop._1().equals("Population");
		});

		JavaDoubleRDD popRdd = rdd2.mapToDouble(city -> {
			Double d = Double.parseDouble(city._1());
			return d;
		});

		JavaDoubleRDD latRdd = rdd2.mapToDouble(city -> {
			Double d = Double.parseDouble(city._2());
			return d;
		});

		JavaDoubleRDD lngRdd = rdd2.mapToDouble(city -> {
			Double d = Double.parseDouble(city._3());
			return d;
		});

		StatCounter popStats = popRdd.stats();
		StatCounter latStats = latRdd.stats();
		StatCounter lngStats = lngRdd.stats();

		System.out.println("Population ==> " +popStats.toString());
		System.out.println("Latitude ==> " +latStats.toString());
		System.out.println("Longitude ==> " + lngStats.toString());

		//initTab();
	}*/

	public void mapper() throws IOException {
		SparkConf conf = new SparkConf().setAppName("PLE LatLng");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd = context.textFile(file);

	
		JavaRDD<Tuple3<String, String, String>> rdd2 = rdd.map(line -> { 
			String[] elements = line.split(",");
			//if(elements[2].equals("")) elements[2] = "-1";
			return new Tuple3<String, String, String>(elements[2], elements[0], elements[1]);
		});
		
		System.out.println("==> COUNT "+rdd2.count());

		JavaPairRDD<String, Integer> result = rdd2
				.filter(pop -> {
					return !pop._1().equals("-1") && !pop._1().equals("Population");
				}).map(pop -> {
					return pop;
				}).mapToPair( f -> {
					int indexAbs = (int)(Double.parseDouble(f._2()) / PIX_SIZE);
					double bornInfAbs = indexAbs * PIX_SIZE;
					double bornSupAbs = (indexAbs + 1) * PIX_SIZE;

					int indexOrd = (int)(Double.parseDouble(f._3()) / PIX_SIZE);
					double bornInfOrd = indexOrd * PIX_SIZE;
					double bornSupOrd= (indexOrd + 1) * PIX_SIZE;

					return new Tuple2<String, Integer>(
							(int)Math.min(bornInfAbs, bornSupAbs)+","+(int)Math.max(bornInfOrd, bornSupOrd),
							Integer.parseInt(f._1()));
				});

		JavaPairRDD<String, Iterable<Integer>> reduce = result.groupByKey();
		
		
		JavaPairRDD<String, Integer> fin = reduce.mapToPair(f -> {
			int sum = 0, count = 0;
			for(Integer i : f._2()) {
				sum +=i;
				count++;
			}
			
			return new Tuple2<String, Integer>(f._1(), (int) sum / count);
		});
		
		//fin.saveAsTextFile("hdfs://beetlejuice:9000/user/hfikri/output_spark");

		final_results = fin.collect();
		System.out.println("RESULT ===> "+final_results);

	}

	public List<Tuple2<String, Integer>> getResults() {
		return final_results;
	}

}
