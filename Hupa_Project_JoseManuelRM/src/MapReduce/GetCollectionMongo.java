package MapReduce;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.ServerAddress;

import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Indexes;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import java.util.Arrays;
import java.util.Date;

import com.mongodb.Block;

import com.mongodb.client.MongoCursor;
import static com.mongodb.client.model.Filters.*;
import com.mongodb.client.result.DeleteResult;
import static com.mongodb.client.model.Updates.*;
import com.mongodb.client.result.UpdateResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.logging.Level;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import static java.util.Collections.singletonList;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

import scala.xml.dtd.Decl;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;
import java.util.StringTokenizer;
import org.apache.log4j.Logger;
import org.bson.Document;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;

public class GetCollectionMongo {
	private static Logger log = Logger.getLogger(Decl.class);

	public void pruebas(String In, String conf) throws IOException {

		MongoClientURI connectionString = new MongoClientURI("mongodb://localhost:27017");
		MongoClient mongoClient = new MongoClient(connectionString);

		MongoDatabase database = mongoClient.getDatabase("Nasa");
		MongoCollection<Document> collection = database.getCollection("satelites");
		
		//Limpio directorios para que no haya archivos en siguientes ejecuciones
		int contDel = 0;
		System.out.println("La carpeta de entrada para generar los CSV con MongoExport es: " + In);
		File fdelete = new File(In + "/jose.csv");
		for (int i = 0; i < 5; i++) {
			if (i == 0) {
			//System.out.println("Se procede a borrar el directorio inicial " + fdelete);
			System.out.println("Se procede a borrar los directorios");
			} else {
				//System.out.println("Se procede a borrar el directorio " + fdelete);
				fdelete = new File(In + "/jose" + contDel + ".csv");
			}
			//"/home/shadoop/Desktop/pruebasCVSJoseM/fin/jose"
			contDel++;
			if (fdelete.exists()) {
				FileUtils.forceDelete(fdelete);
			}	
		}
		//0,40,30,70,2015008,2015010
		//-60,0,-58,10,2015005,2015007
		
		// Creo una lista de regiones
		List<Region> listaR = new ArrayList<Region>();
		// Creo un buffer de lectura del archivo de configuración.txt
		BufferedReader br = new BufferedReader(new FileReader(conf));
		// Leo la primera línea del archivo de configuración.txt
		String line2 = br.readLine();

		float lat1, lat2, long1, long2;
		int fecha1 = 0, fecha2 = 0, contFecha = 1, contit = 0;
		//contador para hacer los mongoexport necesarios
		int cont = 0;
		String command = "";
		// Bucle donde voy leyendo líneas, por cada línea se crea una nueva región con
		// los puntos indicados por las líneas
		while (line2 != null) {
			Region r = new Region();

			// El tokenizer es la ,
			StringTokenizer st2 = new StringTokenizer(line2, ",");

			// Punto1
			r.setLatPunto1(Float.parseFloat((String) st2.nextElement()));
			r.setLonPunto1(Float.parseFloat((String) st2.nextElement()));
			// Punto2
			r.setLatPunto2(Float.parseFloat((String) st2.nextElement()));
			r.setLonPunto2(Float.parseFloat((String) st2.nextElement()));

			fecha1 = Integer.parseInt((String) st2.nextElement());
			fecha2 = Integer.parseInt((String) st2.nextElement());

			// obtengo las latitudes y longitudes
			lat1 = r.getLatPunto1();
			lat2 = r.getLatPunto2();
			long1 = r.getLonPunto1();
			long2 = r.getLonPunto2();
			System.out.println("Los datos son --> lat1: " + lat1 + " lat2: " + lat2 + " long1: " + long1 + " long2: "
					+ long2 + " Fecha inicial: " + fecha1 + " Fecha final: " + fecha2);
	
			if (cont == 0) {
				command =  "mongoexport --host localhost:27017 --db Nasa --collection satelites --type=csv --fields Tiempo,latitud,longitud,wind_Speed,wind_dir --query {Tiempo:{$gt:" + "\""+ fecha1 +"\"" + ",$lt:" + "\"" + fecha2 + "\"" + "},latitud:{$gt:" + lat1 + ",$lt:" + lat2 + "},longitud:{$gt:" + long1 + ",$lt:" + long2 + "}} --out " + In + "/jose.csv";
			} else {
				command =  "mongoexport --host localhost:27017 --db Nasa --collection satelites --type=csv --fields Tiempo,latitud,longitud,wind_Speed,wind_dir --query {Tiempo:{$gt:" + "\""+ fecha1 +"\"" + ",$lt:" + "\"" + fecha2 + "\"" + "},latitud:{$gt:" + lat1 + ",$lt:" + lat2 + "},longitud:{$gt:" + long1 + ",$lt:" + long2 + "}} --out " + In + "/jose" + cont + ".csv";
			}
			cont++;
			//FUNCIONA: command =  "mongoexport --host localhost:27017 --db Nasa --collection satelites --type=csv --fields Tiempo,latitud,longitud,wind_Speed,wind_dir --query {Tiempo:{$gt:" + "\""+ fecha1 +"\"" + ",$lt:" + "\"" + fecha2 + "\"" + "},latitud:{$gt:" + lat1 + ",$lt:" + lat2 + "},longitud:{$gt:" + long1 + ",$lt:" + long2 + "}} --out /home/shadoop/Desktop/pruebasCVSJoseM/fin/jose.csv";

			String commandfin = command;
			System.out.println("commandfin: " + commandfin);
			System.out.println("### el comando es: #####");
			System.out.println(command);
			System.out.println("#######################");
			
			
			try {
				Process process = Runtime.getRuntime().exec(command);
				int waitfor = process.waitFor();
				System.out.println("waitFor:: " + waitfor);
				BufferedReader success = new BufferedReader(new InputStreamReader(process.getInputStream()));
				BufferedReader error = new BufferedReader(new InputStreamReader(process.getErrorStream()));
				
				String s = "";
				while ((s = success.readLine()) != null) {
					System.out.println(s);
				}
				while ((s = error.readLine()) != null) {
					System.out.println("Exportando desde MongoDB: " + s);
				}
			
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			// Leo nueva línea
			line2 = br.readLine();
			// Añado la región a la lista de regiones
			listaR.add(r);
		
		}
		System.out.println("##### Salgo del bucle ######");
	

		System.out.println("fin ");

	}
/*
	public static void main(String[] args) throws IOException {
		GetCollectionMongo n = new GetCollectionMongo();
		String conf = "output/conf/ConfiguracionMongoSpark.txt";
		String in = "/home/shadoop/Desktop/pruebasCVSJoseM/fin/jose";
		n.pruebas(in, conf);
	}*/

}
