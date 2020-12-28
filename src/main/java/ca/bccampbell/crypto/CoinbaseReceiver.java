package ca.bccampbell.crypto;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PipedReader;
import java.io.PipedWriter;
import java.io.IOException;
import java.net.ConnectException;
import java.net.Socket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashMap;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.List;
import java.util.ArrayList;
import java.sql.*;
import java.math.BigInteger;
import com.google.common.io.Closeables;

import ca.bccampbell.crypto.CoinbaseClient.EventWebSocket;

import org.apache.spark.sql.SparkSession;
import org.apache.spark.SparkConf;
import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.Duration;
import org.apache.spark.streaming.api.java.JavaDStream;
import org.apache.spark.streaming.api.java.JavaPairDStream;
import org.apache.spark.streaming.api.java.JavaReceiverInputDStream;
import org.apache.spark.streaming.api.java.JavaStreamingContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.receiver.Receiver;

import org.eclipse.jetty.websocket.api.Session;
import org.json.JSONArray;
import org.json.JSONObject;

import org.apache.log4j.Logger;
import org.apache.log4j.Level;

public class CoinbaseReceiver extends Receiver<String> {
	
	private static CoinbaseClient coinbaseClient;
	
	public static void main(String[] args) throws IOException {
		CoinbaseReceiver cbr = new CoinbaseReceiver();
		cbr.run();
	}
	public void run() throws IOException {
		try {
			Logger.getLogger("org").setLevel(Level.ERROR);
			Logger.getLogger("akka").setLevel(Level.ERROR);
			
			SparkSession spark = SparkSession.builder()
			      									.appName("CoinbaseReceiver")
			      									.master("local[2]")
			      									.getOrCreate();

   	   List<String> fieldNames = Arrays.asList("type","sequence","product_id","price","open_24h","volume_24h","low_24h","high_24h","volume_30d","best_bid","best_ask","side","time","trade_id","last_size");
   	   List<String> fieldTypes = Arrays.asList("string","long","string","float","float","double","float","float","double","float","float","string","datetime","int","float");
   	   
   	   createTickerTable();
   	   
			JavaStreamingContext jsc = new JavaStreamingContext(new JavaSparkContext(spark.sparkContext())
																			  , new Duration(1000));		
   	   JavaReceiverInputDStream<String> feed = jsc.receiverStream(new CoinbaseReceiver());
   	   JavaDStream<Row> ticker = feed.filter(s -> new JSONObject(s).getString("type").equals("ticker"))
   	   										   .map(s -> jsonStringToRow(s, fieldNames, fieldTypes));   	   
   	   
   	   // Generate the DataFrame schema
   	   List<StructField> fields = new ArrayList<>();
   	   
      	for (int i = 0; i < fieldNames.size(); i++) {
      	   if (fieldTypes.get(i) == "int") {
   	   	   StructField field = DataTypes.createStructField(fieldNames.get(i), DataTypes.IntegerType, true);
      	   	fields.add(field);
      	   } else if (fieldTypes.get(i) == "long") {
   	   	   StructField field = DataTypes.createStructField(fieldNames.get(i), DataTypes.LongType, true);
      	   	fields.add(field);
      	   } else if (fieldTypes.get(i) == "float") {
   	   	   StructField field = DataTypes.createStructField(fieldNames.get(i), DataTypes.FloatType, true);
      	   	fields.add(field);
      	   } else if (fieldTypes.get(i) == "double") {
   	   	   StructField field = DataTypes.createStructField(fieldNames.get(i), DataTypes.DoubleType, true);
      	   	fields.add(field);
      	   } else {
   	   	   StructField field = DataTypes.createStructField(fieldNames.get(i), DataTypes.StringType, true);
      	   	fields.add(field);
      	   }
   	   }
   	   StructType schema = DataTypes.createStructType(fields);
   	   
   	   
   	   ticker.foreachRDD(rdd -> DFtoMySQL(spark.createDataFrame(rdd, schema))); 
   	   ticker.print();
   	   jsc.start();
   	   jsc.awaitTermination();	   
		} catch (Exception e) {
			e.printStackTrace();
		}
		//CoinbaseReceiver cbr = new CoinbaseReceiver();
		//cbr.runReader();
	}
	
	public CoinbaseReceiver() {
	    super(StorageLevel.MEMORY_AND_DISK_2());
	}
	
   @Override
   public void onStart() {
      new Thread(this::receive).start();
   }
   @Override
   public void onStop() {}

   private void receive() {
      try {
  	      coinbaseClient = new CoinbaseClient();
  	      coinbaseClient.runCoinbase();
         BufferedReader reader = new BufferedReader(coinbaseClient.getReader());
         try {
           String feedInput;
           while (!isStopped() && (feedInput = reader.readLine()) != null) {
              JSONObject obj = new JSONObject(feedInput);
        	     store(feedInput);
           }
         } finally {
           Closeables.close(reader, true);
         }
      } catch (Exception e) {
      	e.printStackTrace();
      }
  }	
   
   public List<String> jsonStringToArrayList(String input) {
   	List<String> list = new ArrayList<>();
   	JSONObject json = new JSONObject(input);
   	JSONArray key = json.names();
   	for (int i = 0; i < key.length(); i++) {
   	   list.add(json.getString(key.getString(i)));
   	}
   	return list;
   }
   
   public Row jsonStringToRow(String input, List<String> fieldNames, List<String> fieldTypes) {

   	Object[] objArray = new Object[fieldTypes.size()];
   	JSONObject json = new JSONObject(input);
   	for (int i = 0; i < fieldNames.size(); i++) {
   	   if (fieldTypes.get(i).equals("int")) {
   	   	objArray[i] = json.getInt(fieldNames.get(i));
   	   } else if (fieldTypes.get(i).equals("long")) {
   	   	objArray[i] = json.getBigInteger(fieldNames.get(i)).longValue();
   	   } else if (fieldTypes.get(i).equals("float")) {
   	   	objArray[i] = json.getFloat(fieldNames.get(i));
   	   } else if (fieldTypes.get(i).equals("double")) {
   	   	objArray[i] = json.getDouble(fieldNames.get(i));
   	   } else if (fieldTypes.get(i).equals("datetime")) {
   	      String datetime = json.getString(fieldNames.get(i));
   	      datetime = (datetime.substring(0, datetime.length() - 1));
   	      objArray[i] = datetime;
   	   } else {
   	      objArray[i] = json.get(fieldNames.get(i));
   	   }
	   }
      return RowFactory.create(objArray);
   }
   
   public void createTickerTable() throws SQLException {
      DriverManager.registerDriver(new com.mysql.jdbc.Driver());
      String mysqlUrl = "jdbc:mysql://localhost/crypto";
      Connection con = DriverManager.getConnection(mysqlUrl, "crypto", "bitcoin");
      Statement stmt = con.createStatement();
      stmt.execute("DROP TABLE IF EXISTS `Ticker`");
      
      String query = "CREATE TABLE `Ticker` (\n"
      		+ "  `type` text,\n"
      		+ "  `sequence` bigint DEFAULT NULL,\n"
      		+ "  `product_id` varchar(10) NOT NULL,\n"
      		+ "  `price` float DEFAULT NULL,\n"
      		+ "  `open_24h` float DEFAULT NULL,\n"
      		+ "  `volume_24h` float DEFAULT NULL,\n"
      		+ "  `low_24h` float DEFAULT NULL,\n"
      		+ "  `high_24h` float DEFAULT NULL,\n"
      		+ "  `volume_30d` float DEFAULT NULL,\n"
      		+ "  `best_bid` float DEFAULT NULL,\n"
      		+ "  `best_ask` float DEFAULT NULL,\n"
      		+ "  `side` varchar(10) DEFAULT NULL,\n"
      		+ "  `time` datetime(6) DEFAULT NULL,\n"
      		+ "  `trade_id` int NOT NULL,\n"
      		+ "  `last_size` float DEFAULT NULL,\n"
      		+ "  PRIMARY KEY (`product_id`,`trade_id`)\n"
      		+ ")";
      stmt.execute(query);  	
   }
   
   public void DFtoMySQL(Dataset<Row> df) {
   	df.write().format("jdbc").mode("append")  
   									 .option("url", "jdbc:mysql://localhost/crypto")
   									 .option("dbtable", "Ticker")
   									 .option("user", "crypto")
   									 .option("password", "bitcoin")
   									 .save();
   	
   }
   
}
