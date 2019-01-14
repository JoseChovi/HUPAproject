package MapReduce;

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
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.classification.LogisticRegression;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.SparkSession;
import org.bson.Document;
import static java.util.Collections.singletonList;

import org.apache.spark.SparkConf;

import org.apache.spark.ml.classification.LogisticRegression;

import org.apache.spark.ml.classification.LogisticRegressionModel;

import org.apache.spark.ml.feature.StringIndexer;

import org.apache.spark.ml.feature.VectorAssembler;

import org.apache.spark.sql.Dataset;

import org.apache.spark.sql.Row;

import org.apache.spark.sql.SparkSession;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Indexes;
import com.mongodb.spark.MongoSpark;
import com.mongodb.spark.rdd.api.java.JavaMongoRDD;

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


//import org.apache.hadoop.yarn.server.webproxy.ProxyUtils._;
//import org.apache.directory.shared.kerberos.components.TypedData;
import org.apache.spark.ml.PipelineStage;

public class MongoSparkTFG2 {

	public void mongoSparkExe(String In, String out, String conf) throws IOException {

		Configuration mongodbConfig = new Configuration();
		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("Nasa");
		MongoCollection<Document> collection = database.getCollection("satelites");
		
		SparkConf csonf = new SparkConf();
		csonf.setAppName("Big Data Process Spark and Mongo");
		csonf.set("spark.driver.allowMultipleContexts", "true");
		csonf.setMaster("local[*]");

		
		final SparkSession sparkSession = SparkSession.builder().config(csonf)
				//.appName("Spark Decision Tree Classifer Demo")
				//.master("local[*]")
				.getOrCreate();

		collection.createIndex(Indexes.geo2dsphere("satelites.latitud", "satelites.longitud"));

		// Añadido para eliminar mensajes de log en la consola
		sparkSession.sparkContext().setLogLevel("ERROR");

		// Llamamos a Mongo y obtenemos los resultados indicados por el fichero
		GetCollectionMongo pm = new GetCollectionMongo();
		pm.pruebas(In, conf);
		// El CSV ya está creado a través de Mongo

		System.out.println("##### Rutas de los ficheros de entrada, salida y configuración ##### \n");
		System.out.println("Archivo de entrada: " + In + "\n");
		System.out.println("Directorio de salida: " + out + "\n");
		System.out.println("Fichero de Configuración: " + conf + "\n");

		// Leo el CSV
		final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
		final Dataset<Row> trainingData = dataFrameReader.csv(In + "/*.csv"); // para el programa
		// principal añadir + "*.csv" para que lea todo el directorio

		// Creación de vista y se ejecuta una query para convertir tipos
		trainingData.createOrReplaceTempView("TRAINING_DATA");
		final Dataset<Row> typedTrainingData = sparkSession
				.sql("SELECT CAST(Tiempo as int) Tiempo, CAST(latitud as float) latitud, "
						+ "CAST(longitud as float) longitud, CAST(wind_Speed as float) wind_Speed, CAST(wind_dir as float) wind_dir FROM TRAINING_DATA"); // CAST(vel_Media

		System.out.println("##### Los Datos cargados desde Mongo son los siguientes ##### con un total de :" + typedTrainingData.count());
		typedTrainingData.show();
		
		//Se obtienen los diferentes días cargados desde Mongo
		System.out.println("##### Los diferentes días cargados desde Mongo son #####");
		typedTrainingData.select("Tiempo").distinct().show();


		System.out.println("##### Cálculo de la Velocidad Media y Velocidad Máxima de cada día #####");
		Dataset<Row> velMaxAVG = sparkSession.sql(
				"SELECT Tiempo, avg(wind_Speed) AS AVG_wind, MAX(wind_Speed) AS MAX_wind FROM TRAINING_DATA GROUP BY Tiempo");

		// JOIN
		// Todos los datos con AVG_wind y MAX_wind están en el dataset Joined
		Dataset<Row> joined = typedTrainingData.join(velMaxAVG, "Tiempo");
		joined.printSchema();

		joined.show();

		// Realización de pruebas para comprobar que se han guardado los datos correctos
		joined.createOrReplaceTempView("pruebasEjecucion");
		System.out.println("##### Realización de pruebas, se consulta un día #####");
		System.out.println("Día 6");
		Dataset<Row> p1 = sparkSession.sql("SELECT * FROM pruebasEjecucion WHERE Tiempo = 2015006");
		p1.show();
		/*System.out.println("Día 8");
		Dataset<Row> p3 = sparkSession.sql("SELECT * FROM pruebasEjecucion WHERE Tiempo = 2015008");
		p3.show();*/
		System.out.println("##### Fin pruebas #####");

		// Creación de Ficheros Weka
		//Se borra el directorio para que no haya archivos antiguos
		File fdelete = new File("output/files2");
		if (fdelete.exists()) {
			FileUtils.forceDelete(fdelete);
		}

		joined.javaRDD().map(Row -> Row.toString().replace("[", "").replace("]", "")).coalesce(1)
				.saveAsTextFile("output/files2");

		System.out.println("##### Ejecución Algoritmo KMeans con Weka #####");

		// Se abre ficheros de lectura y de escritura con los I/O
		String rutaDest = "output/wekafiles/outWekaMongo.arff";
		FileWriter ficheroMax = null;
		try {
			ficheroMax = new FileWriter(rutaDest);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		PrintWriter pwMax = new PrintWriter(ficheroMax);

		// Se inserta cabecera en el archivo .arff
		insertHeadARFF(pwMax);

		// Se lee el fichero txt y se vuelca en pwMax.
		FileReader fr = new FileReader("output/files2/part-00000");

		String line;

		try {

			BufferedReader bufferreader = new BufferedReader(new FileReader("output/files2/part-00000"));

			while ((line = bufferreader.readLine()) != null) {
				pwMax.println(line);
			}

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
	
		try

		{
			ficheroMax.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		//Ejecución de Weka
		runWeka("/home/shadoop/eclipse-workspaceJOse/TFG_BigGeoData_JoseManuelRM/output/wekafiles/", out); // outWeka.arff

		System.out.println("##### Fin Ejecución Algortimo KMeans de Weka #####");

	    System.out.println("##### Algoritmo de DecisionTree de Apache Spark MLlib #####");
	    
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "latitud", "longitud", "wind_Speed", "wind_dir", "AVG_wind", "MAX_wind" })
				.setOutputCol("features");


		joined.createOrReplaceTempView("AlgorithmSpark");

		Dataset<Row> algSpark = sparkSession.sql("SELECT CAST(Tiempo as int) Tiempo, CAST(latitud as float) latitud, "
				+ "CAST(longitud as float) longitud, CAST(wind_Speed as float) wind_Speed, CAST(wind_dir as float) wind_dir, CAST(AVG_wind as float) AVG_wind, CAST(MAX_wind as float) MAX_wind FROM AlgorithmSpark");

		System.out.println("##### El Dataset que se vectoriza es el siguiente #####");
		algSpark.show();
		
		final Dataset<Row> featuresData = vectorAssembler.transform(algSpark);
		// Print Schema para ver los nombres de las columnas, tipos y otros datos
		System.out.println("##### Esquema tras vectorizar los datos del Dataset #####");
		featuresData.printSchema();

		//La indexación se realiza para mejorar los tiempos de ejecución, ya que comparar índices es mucho más barato que comparar String / Float
		
		//Indexe las etiquetas, agregando metadatos a la columna de la etiqueta (dirViento). Ajuste en el conjunto de datos completo para incluir todas las etiquetas en el índice.
		
		// Se añade metadata a la columna (wind_Speed). Se ajusta
		// todo el dataset incluyendo todas las etiquetas en el índice
		final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("wind_Speed")
				.setOutputCol("indexedLabel").fit(featuresData);

		// Índice de vectores de características
		final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features")
				.setOutputCol("indexedFeatures").fit(featuresData);

		// Se dividen los datos en conjuntos de entrenamiento y pruebas (el 30% para las pruebas).
		Dataset<Row>[] splits = featuresData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingFeaturesData = splits[0];
		Dataset<Row> testFeaturesData = splits[1];
		
		
		
		// Train a DecisionTree model.
		final DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel")
				.setFeaturesCol("indexedFeatures");

		//Convertir etiquetas indexadas de nuevo a etiquetas originales
		final IndexToString labelConverter = new IndexToString().setInputCol("prediction")
				.setOutputCol("predictedOccupancy").setLabels(labelIndexer.labels());

		// Encadenar indexadores y árbol en una pipeline.
		final Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });

		
		//Se prueba y los indexadores también
		final PipelineModel model = pipeline.fit(trainingFeaturesData);

		// Make predictions.
		final Dataset<Row> predictions = model.transform(testFeaturesData);

		// Se seleccionan las filas para el ejemplo.
		System.out.println("Example records with Predicted wind_Speed as 0:");
		predictions.select("predictedOccupancy", "wind_Speed", "features")
				.where(predictions.col("predictedOccupancy").equalTo(0)).show(10);

		System.out.println("Example records with Predicted wind_Speed as 1:");
		predictions.select("predictedOccupancy", "wind_Speed", "features")
				.where(predictions.col("predictedOccupancy").equalTo(1)).show(10);

		// En este vuelvo el resultado a un fichero CSV
		System.out.println("Example records with In-correct predictions:");
		predictions.select("predictedOccupancy", "wind_Speed", "features")
				.where(predictions.col("predictedOccupancy").notEqual(predictions.col("wind_Speed"))).show();
		// para escribir se usa lo de abajo, solo interesa mostrar por lo que uso show
		/*
		 * .write() .format("com.databricks.spark.csv").option("header",
		 * "true").mode("overwrite") .save("output/algoritmo.csv");
		 */		

		System.out.println("#### Fin de la ejecución del Algortimo Decision Tree de MLlib Spark #####");

	}

	/**
	 * Inserta la cabecera en los ficheros arff.
	 *
	 * @param pw es el printwrite sobre el que se inserta la cabecera
	 */
	private void insertHeadARFF(PrintWriter pw) {
		pw.println("% ARFF");
		pw.println("% Fichero para Weka con los datos del RS de 2017 y AS de 2018");

		pw.println("@RELATION rapidscat_ascat");
		pw.println("@ATTRIBUTE fechaJuliana numeric");
		// pw.println("@ATTRIBUTE satelite numeric");
		// pw.println("@ATTRIBUTE fechaGregorian date \"yyyy-MM-dd\"");
		// pw.println("@ATTRIBUTE id string");
		pw.println("@ATTRIBUTE latitud numeric");
		pw.println("@ATTRIBUTE longitud numeric");
		pw.println("@ATTRIBUTE velocidad_viento numeric");
		pw.println("@ATTRIBUTE dirección_viento numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMedia numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMax numeric");
		pw.println("@DATA");
	}

	// Ejecución de Weka
	private static void runWeka(String rutaFuente, String out) {

		// String command = "java -classpath
		// $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar weka.Run WekaForecaster -W
		// \"weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8
		// -num-decimal-places 4\" -t "
		// + rutaFuente + "*.arff " + " -F velocidad_vientoMax -L 1 -M 2 -G fechaJuliana
		// -prime 2";

		String command = "java -classpath $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar  weka.clusterers.SimpleKMeans -N 10 -t "
				+ rutaFuente + "*.arff";
		// -N es el número de clústers, tienen asignados 6
		String ruta = rutaFuente.substring(0, rutaFuente.length() - 5);
		System.out.println("Esto tiene ruta fuente: " + rutaFuente);
		String rutaFinal = out;// "output/wekafiles.txt";//ruta + ".txt";
		FileWriter fichero = null;
		try {
			fichero = new FileWriter(rutaFinal + "/wekafiles.txt");
			String output = executeCommand(command);
			System.out.println("Ejecución del algoritmo SimpleKMeans: " + output);
			fichero.write(output);
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		try {
			fichero.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	// Ejecución de comandos, usado para ejecutar Weka
	private static String executeCommand(String command) {

		StringBuffer output = new StringBuffer();

		Process p;
		try {
			p = Runtime.getRuntime().exec(new String[] { "bash", "-c", command });
			p.waitFor();
			BufferedReader reader = new BufferedReader(new InputStreamReader(p.getInputStream()));

			String line = "";
			while ((line = reader.readLine()) != null) {
				output.append(line + "\n");
			}

		} catch (Exception e) {
			e.printStackTrace();
		}

		return output.toString();

	}

	/*public static void main(String[] args) throws IOException {
		MongoSparkTFG2 xd = new MongoSparkTFG2();
		String in = "/home/shadoop/Desktop/pruebasCVSJoseM/fin/";
		String out = "output/CSVs"; // donde se generan los arff
		String conf = "output/conf/ConfiguracionMongoSpark.txt";
		xd.mongoSparkExe(in, out, conf);

	}*/

}
