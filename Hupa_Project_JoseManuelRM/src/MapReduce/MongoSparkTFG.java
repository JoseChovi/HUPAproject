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

public class MongoSparkTFG {

	public void mongoSparkExe(String out, String conf) throws IOException {

		MongoClient mongoClient = new MongoClient();
		MongoDatabase database = mongoClient.getDatabase("Nasa");
		MongoCollection<Document> collection = database.getCollection("satelites");

		// http://mongodb.github.io/mongo-java-driver/3.6/javadoc/?com/mongodb/client/model/Indexes.html
		collection.createIndex(Indexes.geo2dsphere("satelites.latitud", "satelites.longitud"));

		SparkSession spark = SparkSession.builder().master("local").appName("MongoSparkConnector")
				.config("spark.mongodb.input.uri", "mongodb://127.0.0.1/Nasa.satelites")
				.config("spark.mongodb.output.uri", "mongodb://127.0.0.1/Nasa.satelites").getOrCreate();

		// Añadido para eliminar mensajes de log en la consola
		spark.sparkContext().setLogLevel("ERROR");

		// Muestro rutas de entrada, salida y configuración
		System.out.println("----------Rutas de los ficheros de entrada, salida y configuración----------\n");
		System.out.println("Directorio de salida: " + out + "\n");
		System.out.println("Fichero de Configuración: " + conf + "\n");

		// Create a JavaSparkContext using the SparkSession's SparkContext object
		JavaSparkContext jsc = new JavaSparkContext(spark.sparkContext());
		System.out.println("--- Se procede a mostrar el esquema y los datos cargados desde la BD NoSQL de Mongo ---");
		/* Start Example: Read data from MongoDB ************************/
		// JavaMongoRDD<Document> rdd = MongoSpark.load(jsc);
		// System.out.println(rdd.first());
		// Dataset<Row> implicitDS = rdd.toDF();

		Dataset<Row> implicitDS = MongoSpark.load(jsc).toDF();
		implicitDS.printSchema();
		implicitDS.show();

		implicitDS.createOrReplaceTempView("Sats");

		// Procedo a consultar la Vel_Media y Vel_Max
		System.out.println("--- Se calcula la velocidad media y máxima por cada día ---");
		// Column ageCol = typedTrainingData.col("vel_Media");
		Dataset<Row> allDataTime = spark.sql(
				"SELECT Tiempo, avg(wind_Speed) AS Vel_Media, MAX(wind_Speed) AS Vel_Max FROM Sats GROUP BY Tiempo");

		// JOIN
		// AQUÍ EN EL DATASET JOINED YA TENGO TODOS LOS DATOS EN UNA MISMA TABLA
		Dataset<Row> joined = implicitDS.join(allDataTime, "Tiempo");
		System.out.println("--- Los resultados son los siguientes: ---");
		joined.show();

		// Creo una lista de regiones
		List<Region> listaR = new ArrayList<Region>();
		// Creo un buffer de lectura del archivo de configuración.txt
		BufferedReader br = new BufferedReader(new FileReader(conf));
		// Leo la primera línea del archivo de configuración.txt
		String line2 = br.readLine();

		// Bucle donde voy leyendo líneas, por cada línea se crea una nueva región con
		// los puntos indicados por las líneas
	
		int fecha1 = 0, fecha2 = 0, contFecha = 1;
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
			
			//con esto me quedo con la primera fecha indicada para todas las regiones introducidas
			//if (st2.nextElement() != "\n") {
			//añadido para fechas
			//if (contFecha == 1) {
			fecha1 = Integer.parseInt((String) st2.nextElement());
			fecha2 = Integer.parseInt((String) st2.nextElement());
			//contFecha--;
		//	}
			//}
			
			// Leo nueva línea
			line2 = br.readLine();
			// Añado la región a la lista de regiones
			listaR.add(r);
		} // Una vez que salgo del bucle ya tengo todas las regiones de las que debo de
			// sacar los puntos
		
		
		if (fecha1 == 0 || fecha2 == 0) {
			 System.out.println("Error en las fechas");
			 System.exit(0);
		}
		
		
		// Recorro la lista de regiones y voy realizando las consultas, declaro antes
		// variables
		float lat1, lat2, long1, long2;

		System.out.println("Las fechas introducidas son: " + fecha1 + " - " + fecha2);
		
		// Create view to execute query to get all data from sats
		joined.createOrReplaceTempView("allDataView");

		// Estos dos Dataset son creados para hacer union e intersect con el fin
		// de obtener en un mismo Dataset los datos que cumplen las condiciones
		// indicadas por el fichero configuracion.txt
		Dataset<Row> datatotxt = null;
		Dataset<Row> finalss = spark.sql("SELECT * FROM allDataView");
		System.out.println("Muestro lo que tiene finalss para hacer la union");
		finalss.show();
		// finalss.show(); //tiene todos los datos

		int cont = 1;
		System.out.println("----------DATOS QUE CUMPLEN CON EL FICHERO DE CONG----------");
		for (int i = 0; i < listaR.size(); i++) {
			lat1 = listaR.get(i).getLatPunto1();
			lat2 = listaR.get(i).getLatPunto2();
			long1 = listaR.get(i).getLonPunto1();
			long2 = listaR.get(i).getLonPunto2();
			System.out.println("lat1: " + lat1 + " lat2: " + lat2 + " long1: " + long1 + " long2: " + long2);

			// Aquí puedo hacer el Select.
			/*
			 * //PARA SACAR EN UN CSV LOS RESULTADOS sparkSession
			 * .sql("SELECT * FROM ROOM_OCCUPANCY WHERE lat >" + lat1 + " AND lat < " + lat2
			 * + " AND long > " + long1 + " AND long < " + long2)
			 * .coalesce(1).write().format("com.databricks.spark.csv").option("header",
			 * "true").mode("append") .save(out);
			 */
			// Con esto hago consulta SQL y la vuelco en un fichero CSV

			// Hago lo mismo para mostrar por pantalla
			System.out.println("Muestro las consultas hechas en el bucle FOR iteracción nº " + i + "\n");
			if (fecha1 != 0 && fecha2 != 0) {
				System.out.println("Fecha es diferente a 0, entro: " + fecha1 + " " + fecha2);
			spark.sql("SELECT * FROM allDataView WHERE latitud > " + lat1 + " AND latitud < " + lat2
					+ " AND longitud > " + long1 + " AND longitud < " + long2 + " AND Tiempo >= " + fecha1 + " AND Tiempo < " + fecha2).show();
			}
			// ESTE ES EL IMPORTANTE YA QUE SON LOS DATOS QUE TRATO
			datatotxt = spark.sql("SELECT * FROM allDataView WHERE latitud > " + lat1 + " AND latitud < " + lat2
					+ " AND longitud > " + long1 + " AND longitud < " + long2);

			if (cont == 1) {
				finalss = finalss.intersect(datatotxt);
				cont--;
			} else {
				// Aquí en finalss voy almacenando los puntos correctos
				finalss = finalss.union(datatotxt);
			}

		}

		System.out.println(
				"----------MUESTRO FUERA DEL BUCLE. EN EL DATASET FINALSS ESTÁN TODOS LOS PUNTOS CORRECTOS----------");
		finalss.show();
		finalss.toString();

		/* CREACIÓN DE FICHERO DE TEXTO PARA WEKA */

		File fdelete = new File("output/files2");
		if (fdelete.exists()) {
			FileUtils.forceDelete(fdelete);
		}
		// deleteDirectory("output/files1/part-00000"); //para borrar directamente el
		// archivo

		finalss.javaRDD().map(Row -> Row.toString().replace("[", "").replace("]", "")).coalesce(1)
				.saveAsTextFile("output/files2");

		System.out.println("----------Start Weka Process----------");

		// Se abre ficheros de lectura y de escritura con los I/O
		String rutaDest = "output/outWekaMongo.arff";
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

		// Aquí debo de leer el fichero txt y volcarlo en pwMax.
		FileReader fr = new FileReader("output/files2/part-00000");

		String line;

		try {

			BufferedReader bufferreader = new BufferedReader(new FileReader("output/files2/part-00000"));

			while ((line = bufferreader.readLine()) != null) {
				/**
				 * Your implementation
				 **/
				pwMax.println(line);
				// line = bufferreader.readLine();
			}

		} catch (FileNotFoundException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}

		runWeka("/home/shadoop/eclipse-workspaceJOse/TFG_JaimeModfdiego/output/wekafiles/", out); // outWeka.arff

		try

		{
			ficheroMax.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Ejecución de Weka

		System.out.println("----------Fin Ejecución Weka----------");

		System.out.println("----------Algorithm MLSpark----------");

		// Combine multiple input columns to a Vector using Vector Assembler
		// utility
		// IMPORTANTE: los vectores NO leen STRINGS
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "latitud", "longitud", "wind_Speed", "wind_dir", "Vel_Media", "Vel_Max" })
				.setOutputCol("features");

		finalss.createOrReplaceTempView("AlgorithmSpark");

		Dataset<Row> algSpark = spark.sql("SELECT CAST(Tiempo as int) Fecha, CAST(latitud as float) latitud, "
				+ "CAST(longitud as float) longitud, CAST(wind_Speed as float) wind_Speed, CAST(wind_dir as float) wind_dir, CAST(Vel_Media as float) Vel_Media, CAST(Vel_Max as float) Vel_Max FROM AlgorithmSpark");

		System.out.println("----------AlgSpark contiene esto: ----------");
		algSpark.show();

		final Dataset<Row> featuresData = vectorAssembler.transform(algSpark);
		// Print Schema to see column names, types and other metadata
		System.out.println("--------IMPRIMIR ESQUEMA TRAS LA TRANSFORMACIÓN CON EL VECTOR--------");
		featuresData.printSchema();

		// Index labels, adding metadata to the label column (dirViento). Fit on
		// whole dataset to include all labels in index.
		final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("wind_Speed")
				.setOutputCol("indexedLabel").fit(featuresData);

		// Index features vector
		final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features")
				.setOutputCol("indexedFeatures").fit(featuresData);

		// Split the data into training and test sets (30% held out for
		// testing).
		Dataset<Row>[] splits = featuresData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingFeaturesData = splits[0];
		Dataset<Row> testFeaturesData = splits[1];

		// Train a DecisionTree model.
		final DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel")
				.setFeaturesCol("indexedFeatures");

		// Convert indexed labels back to original labels.
		final IndexToString labelConverter = new IndexToString().setInputCol("prediction")
				.setOutputCol("predictedOccupancy").setLabels(labelIndexer.labels());

		// Chain indexers and tree in a Pipeline.
		final Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });

		// Train model. This also runs the indexers.
		final PipelineModel model = pipeline.fit(trainingFeaturesData);

		// Make predictions.
		final Dataset<Row> predictions = model.transform(testFeaturesData);

		// Select example rows to display.
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
		// .show(10); con esto muestro

		// predictions.write().text(sal);

		System.out.println("----------End of a MLSpark Process Execution----------");

		System.out.println("Fin proceso de ejemplo");
		// Cierro JavaSparkContext
		jsc.close();

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
		pw.println("@ATTRIBUTE satelite numeric");
		// pw.println("@ATTRIBUTE fechaGregorian date \"yyyy-MM-dd\"");
		pw.println("@ATTRIBUTE id string");
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

		String command = "java -classpath $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar weka.Run WekaForecaster -W \"weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8 -num-decimal-places 4\" -t "
				+ rutaFuente + "*.arff " + " -F velocidad_vientoMax -L 1 -M 2 -G fechaJuliana -prime 2";
		String ruta = rutaFuente.substring(0, rutaFuente.length() - 5);
		System.out.println("Esto tiene ruta fuente: " + rutaFuente);
		String rutaFinal = out;// "output/wekafiles.txt";//ruta + ".txt";
		FileWriter fichero = null;
		try {
			fichero = new FileWriter(rutaFinal + "/wekafiles.txt");
			String output = executeCommand(command);
			System.out.println("Mostrando lo que tiene output: " + output);
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

	public static void main(String[] args) throws IOException {
		MongoSparkTFG xd = new MongoSparkTFG();
		String out = "output/CSVs"; // donde se generan los arff
		String conf = "output/conf/ConfiguracionMongoSpark.txt";
		xd.mongoSparkExe(out, conf);

	}

}
