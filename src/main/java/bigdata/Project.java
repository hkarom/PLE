package bigdata;

import java.io.DataInputStream;
import java.io.IOException;
import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaDoubleRDD;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.input.PortableDataStream;


public class Project {

	public static void launch(String[] args) throws IOException {
		
		/*SparkConf conf = new SparkConf().setAppName("PLE Project");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaPairRDD<String, PortableDataStream> rdd;
		rdd = context.binaryFiles(args[0]);

		byte b[] = new byte[1000];
		final int srtm_ver = 1201;
	    int height[][] = new int[1201][1201];
		JavaRDD<PortableDataStream> files = rdd.values();
		System.out.println("HERE0  ");
	    files.foreach(data -> {  	
		try {
			DataInputStream file =  data.open();
			String filename = data.getPath();
			System.out.println("HERE ===> "+filename);
			byte buffer [] = new byte[2];
			filename = filename.substring(filename.length() - 11, filename.length());
			double lat = Double.parseDouble(filename.substring(1,2));
			double lng = Double.parseDouble(filename.substring(4,6));
			

			if(filename.toCharArray()[0] == 'S' || filename.toCharArray()[0] == 's') lat*=-1;
			if(filename.toCharArray()[3] == 'W' || filename.toCharArray()[3] == 'w') lng*=-1;
			DecimalFormatSymbols sym = DecimalFormatSymbols.getInstance();
			 sym.setDecimalSeparator('.');
			DecimalFormat df = new DecimalFormat();
			df.setMaximumFractionDigits(10);
			df.setDecimalFormatSymbols(sym);
            
			for (int i = 0; i<srtm_ver; ++i){
	            for (int j = 0; j < srtm_ver; ++j) {	         
	            	file.read(buffer,0,2);
	            	height[i][j] = (buffer[0] << 8) | buffer[1];
	            	System.out.println(df.format(lat + (double)i * 1. / (double)srtm_ver) + "," + df.format(lng + (double)j * .001 / (double)srtm_ver) + "," + height[i][j] );
	            }
	        }
		}
		catch(Exception e ) {
			System.out.println("ERROR ===>"+ e);
		}
	    });*/
		/*SparkConf conf = new SparkConf().setAppName("TP Spark");
		JavaSparkContext context = new JavaSparkContext(conf);
		JavaRDD<String> rdd;
		rdd = context.textFile(args[0]);
		System.out.println("COUCOU");
		System.out.println("----------------------------------"+rdd.getNumPartitions());
		rdd = rdd.repartition(12);
		
		JavaRDD<String> lines = context.textFile(args[0]);
		JavaRDD<Tuple2<String, String>> pairs = lines.map(l -> { 
								String[] elements = l.split(",");
								String val = elements[4];
								if(elements[4].equals("")) val = "-1";	
									return new Tuple2(elements[3], val);
		
							});
		
		pairs = pairs.filter(tuple -> {
				return !tuple._2.equals("-1") && !tuple._2.equals("Population");
		});
		
		//pairs.collect()
		
		/*for(int i = 0; i< 6; i++) {
			JavaDoubleRDD jdrdd = pairs.mapToDouble(tuple -> {
				Double d = Double.parseDouble(tuple._2);
				return d;
			}).filter(val -> { return val <= 10;});
		}
		*/
	
		
		/*JavaDoubleRDD jdrdd = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val <= 10;});
		
		JavaDoubleRDD jdrdd2 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 10 && val <=100;});;
		
		JavaDoubleRDD jdrdd3 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 100 && val <= 1000;});;
		
		JavaDoubleRDD jdrdd4 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 1000 && val <= 10000;});
		
		JavaDoubleRDD jdrdd5 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 10000 && val <= 100000;});;
		
		JavaDoubleRDD jdrdd6 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 100000 && val <= 1000000;});;
		
		JavaDoubleRDD jdrdd7 = pairs.mapToDouble(tuple -> {
			Double d = Double.parseDouble(tuple._2);
			return d;
		}).filter(val -> { return val > 1000000;});;
		
		
		
	
		StatCounter st = jdrdd.stats();
		StatCounter st2 = jdrdd2.stats();
		StatCounter st3 = jdrdd3.stats();
		StatCounter st4 = jdrdd4.stats();
		StatCounter st5 = jdrdd5.stats();
		StatCounter st6 = jdrdd6.stats();
		StatCounter st7 = jdrdd7.stats();
		System.out.println(st.toString());
		System.out.println(st2.toString());
		System.out.println(st3.toString());
		System.out.println(st4.toString());
		System.out.println(st5.toString());
		System.out.println(st6.toString());
		System.out.println(st7.toString());
	
		System.out.println("LIGNES VALIDES : "+pairs.count());
		
		*/
		
		LatLng stats = new LatLng(args[0]);
		//stats.means();
		stats.mapper();
	}
	
}
