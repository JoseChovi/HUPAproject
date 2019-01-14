package MapReduce;

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
import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.util.BatchedWriteAheadLog.Record;

import scala.collection.Seq;

import org.apache.commons.io.FileUtils;

/**
 * 
 * @author JoseCHovi
 */
public class Dtree2 {

	/**
	 * @param args
	 * @throws IOException añadido el día 22 cuando empecé con Weka
	 */
	public void sparkProcess(String In, String out, String conf) throws IOException {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Spark Decision Tree Classifer Demo")
				.master("local[*]").getOrCreate();

		// Añadido para eliminar mensajes de log en la consola
		sparkSession.sparkContext().setLogLevel("ERROR");
		// fin mensajes log consola

		// String sal = "/output";
		// String rutaDest= out + "/Salidajm";
		// Get DataFrameReader using SparkSession and set header option to true
		// to specify that first row in file contains name of columns
		final DataFrameReader dataFrameReader = sparkSession.read().option("header", false);
		/*
		 * dataFrameReader = StructType[( StructField("member_srl", IntegerType(), True)
		 * )];
		 */
		final Dataset<Row> trainingData = dataFrameReader.csv(In + "/*.csv").toDF("Satelite", "Fecha", "lat", "long",
				"velViento", "dirViento"); // para el programa
		// principal añadir + "*.csv" para que lea todo el directorio

		// Añado columnas aquí
		// AÑADO DOS COLUMNAS, VEL MÁX Y VEL MEDIA
		// NO ES NECESARIO AÑADIRLAS ASÍ YA
		/*
		 * System.out.println("AÑADO DOS NUEVAS COLUMNAS"); Dataset<Row> newDs =
		 * trainingData.withColumn("vel_Media",functions.lit(1)); Dataset<Row> finalDs =
		 * newDs.withColumn("vel_Max",functions.lit(1));
		 * System.out.println("FIN AÑADO NUEVA COLUMNAS");
		 */

		/*
		 * DATOS CON LOS QUE SE TRABAJA EN EL PROYECTO - Satelite - Día (Juliana) -
		 * Latitud - Longitud - Velocidad Viento - Dirección Viento
		 */

		System.out.println("----------Rutas de los ficheros de entrada, salida y configuración----------\n");
		System.out.println("Archivo de entrada: " + In + "\n");
		System.out.println("Directorio de salida: " + out + "\n");
		System.out.println("Fichero de Configuración: " + conf + "\n");

		// Create view and execute query to convert types as, by default, all
		// columns have string types
		trainingData.createOrReplaceTempView("TRAINING_DATA");
		final Dataset<Row> typedTrainingData = sparkSession
				.sql("SELECT CAST(Satelite as String) Satelite, CAST(Fecha as int) Fecha, CAST(lat as float) lat, "
						+ "CAST(long as float) long, CAST(velViento as float) velViento, CAST(dirViento as float) dirViento FROM TRAINING_DATA"); // CAST(vel_Media
																																					// as
																																					// float)
																																					// vel_Media,
																																					// CAST(vel_Max
																																					// as
																																					// float)
																																					// vel_Max

		// FIN PRUEBAS VEL_MEDIA

		/* PRUEBAS CONSULTAS SPARK SQL */
		// Print Schema to see column names, types and other metadata
		typedTrainingData.printSchema();
		System.out.println("----------Datos cargados del CSV----------");
		typedTrainingData.show();

//	typedTrainingData.write().csv("/output/consulta.csv");

		// PRUBEAS VEL_MEDIA
		System.out.println("CALCULO VEL_MEDIA Y MAX_VEL");
		// Column ageCol = typedTrainingData.col("vel_Media");
		Dataset<Row> velMaxAVG = sparkSession.sql(
				"SELECT Fecha, avg(velViento) AS Vel_Media, MAX(velViento) AS Vel_Max FROM TRAINING_DATA GROUP BY Fecha");

		// JOIN
		// AQUÍ EN EL DATASET JOINED YA TENGO TODOS LOS DATOS EN UNA MISMA TABLA
		Dataset<Row> joined = typedTrainingData.join(velMaxAVG, "Fecha");
		joined.show();

		// typedTrainingData.col("vel_Media").; // in Java

		System.out.println(".............PRUEBA OBTENER DATOS DEL DATASET PARA PASARLOS A .ARFF");
//		List<String> listOne = joined.as(Encoders.STRING()).collectAsList();
//	    System.out.println(listOne);

		// ERROR java.lang.OutOfMemoryError: GC overhead limit exceeded
		/*
		 * List<String> listTwo = joined.map(row -> row.mkString(),
		 * Encoders.STRING()).collectAsList();
		 * 
		 * // System.out.println(listTwo);
		 * 
		 * for (int i = 0; i < listTwo.size(); i++) { //
		 * System.out.println(listTwo.get(i)); }
		 * 
		 * String gotsd = joined.first().get(0).toString();
		 */
		// System.out.println(cabra);
		// ERROR java.lang.OutOfMemoryError: GC overhead limit exceeded

		/*
		 * joined.foreach((ForeachFunction<Row>) row -> System.out.println(row)
		 * //toString()
		 * 
		 * );
		 */
		// System.out.println(row));

		/*
		 * StringBuilder sb = new StringBuilder(); sb.AppendLine(String.Join(",",
		 * row.ItemArray));
		 * 
		 * foreach (Dataset<row> row in results.Tables[0].Rows);{
		 * sb.AppendLine(string.Join(",", row.ItemArray)); }
		 * 
		 * String output1 = (string)rows["ColumnName"];
		 */

		/*
		 * List<String> list = joined.as(Encoders.STRING()).collectAsList();
		 * Dataset<String> df1 = sparkSession.createDataset(list, Encoders.STRING());
		 * df1.show();
		 */

		System.out.println("..............FIN PRUEBA OBTENER DATOS DEL DATASET PARA PASARLOS A .ARFF");

		/*
		 * //CONFIGURACIÓN PARA OBTENER DATOS DEL FICHERO CONFIGURACIÓN.TXT
		 */
		// Creo una lista de regiones
		List<Region> listaR = new ArrayList<Region>();
		// Creo un buffer de lectura del archivo de configuración.txt
		BufferedReader br = new BufferedReader(new FileReader(conf));
		// Leo la primera línea del archivo de configuración.txt
		String line2 = br.readLine();

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

			// Leo nueva línea
			line2 = br.readLine();
			// Añado la región a la lista de regiones
			listaR.add(r);
		} // Una vez que salgo del bucle ya tengo todas las regiones de las que debo de
			// sacar los puntos

		// Recorro la lista de regiones y voy realizando las consultas, declaro antes
		// variables
		float lat1, lat2, long1, long2;

		// Create view to execute query to get filtered data
		joined.createOrReplaceTempView("ROOM_OCCUPANCY");

		// Estos dos Dataset son creados para hacer union e intersect con el fin
		// de obtener en un mismo Dataset los datos que cumplen las condiciones
		// indicadas por el fichero configuracion.txt
		Dataset<Row> datatotxt = null;
		Dataset<Row> finalss = sparkSession.sql("SELECT * FROM ROOM_OCCUPANCY");
		// finalss.show(); //tiene todos los datos

		int cont = 1;
		System.out.println("----------DATOS QUE CUMPLEN CON EL FICHERO DE CONG----------");
		for (int i = 0; i < listaR.size(); i++) {
			lat1 = listaR.get(i).getLatPunto1();
			lat2 = listaR.get(i).getLatPunto2();
			long1 = listaR.get(i).getLonPunto1();
			long2 = listaR.get(i).getLonPunto2();

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
			sparkSession.sql("SELECT * FROM ROOM_OCCUPANCY WHERE lat >" + lat1 + " AND lat < " + lat2 + " AND long > "
					+ long1 + " AND long < " + long2).show();

			// ESTE ES EL IMPORTANTE YA QUE SON LOS DATOS QUE TRATO
			datatotxt = sparkSession.sql("SELECT * FROM ROOM_OCCUPANCY WHERE lat >" + lat1 + " AND lat < " + lat2
					+ " AND long > " + long1 + " AND long < " + long2);

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
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		
		//volcado de archivos para Weka

		// Tratando de convertirlo a JAVARDD
		/*
		 * JavaRDD<Row> dataxd = finalss.toJavaRDD();
		 * System.out.println("kereuuuuuuuuuuu"); //dataxd.saveAsTextFile(path,
		 * codec);("/output/CSVs"); System.out.println(dataxd.toString());
		 */

		// Con esto vuelco toda la información del Dataset final a un único archivo .txt

		/*
		 * File index = new File("/output/files1"); String[]entries = index.list();
		 * System.out.println(entries); for(String s: entries){ File currentFile = new
		 * File("/output/files1",s); currentFile.delete(); }
		 */
		// deleteDirectory("output/files1");
		File fdelete = new File("output/files1");
		if (fdelete.exists()) {
			FileUtils.forceDelete(fdelete);
		}
		// deleteDirectory("output/files1/part-00000"); //para borrar directamente el
		// archivo

		finalss.javaRDD().map(Row -> Row.toString().replace("[", "").replace("]", "")).coalesce(1)
				.saveAsTextFile("output/files1");

		System.out.println("----------Start Weka Process----------");

		// Se abre ficheros de lectura y de escritura con los I/O
		String rutaDest = "output/outWeka.arff";
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
		FileReader fr = new FileReader("output/files1/part-00000");

		String line;

		try {

			BufferedReader bufferreader = new BufferedReader(new FileReader("output/files1/part-00000"));

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

		/*
		 * int i; while ((i = fr.read()) != -1) { pwMax.println((char) i); }
		 */

		// System.out.print((char) i);

		// Recorrer todo el fichero e insertarlo en el txt
		// pwMax.println(tmapMax.get(key1).toString());
		// pwMax.println("Prueba para Weka de Jose");
		// Fichero completo con los datos se procede a cerrar
		try

		{
			ficheroMax.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		// Ejecución de Weka

		System.out.println("----------Fin Ejecución Weka----------");

		/*
		 * FileWriter ficheroMaxwe = null; ficheroMaxwe = new
		 * FileWriter("output/algoritmos.txt"); PrintWriter pwMawex = new
		 * PrintWriter(ficheroMaxwe); pwMawex.println(finalss.showString(5, false));
		 * pwMawex.close();
		 */

		// List<String> listt =
		// System.out.println(finalss.map(row -> row.mkString(),
		// Encoders.STRING()).collectAsList().toString());
		// System.out.println("INtentando volcar info datasets:::::_________wscweaf
		// szx_________::::::::_");
		// pwMawex.println
		// finalss.foreach((ForeachFunction<Row>) row -> System.out.println(row));
		// //System.out.println(row

		// System.out.println(listt);

		// PRUEBA LECTOR CSV
		/*
		 * System.out.println("PRUEBA PARSER CSV"); Readercsv n = new Readercsv();
		 * n.main(null); System.out.println("FIN PRUEBA PARSER CSV");
		 */

		// System.out.println("wwwwwwwwwwwwwwwwwwwwwwwwwwwwwwwww: ");
		// LEER AQUÍ. DEBO DE JUNTAR LOS CSV RESULTANTES
		// EN UNO SOLO, UNA VEZ TENGA ESTE, COMIENZO A TRABAJAR TANTO WEKA
		// COMO EL ALGORITMO PROPIO DE SPARK CON ESTE CSV

		/*
		 * FIN CONFIGURACIÓN PARA OBTENER DATOS DEL FICHERO CONFIGURACIÓN.TXT
		 * 
		 */
		// Aquí puedo hacer el Select.
		/*
		 * sparkSession.
		 * sql("SELECT * FROM ROOM_OCCUPANCY WHERE lat BETWEEN 6 AND 7 AND long BETWEEN 113 AND 115"
		 * ).write() .format("com.databricks.spark.csv").option("header",
		 * "true").mode("overwrite").save(out);
		 */
		// Con esto hago consulta SQL y la vuelco en un fichero CSV

		// show(); esto solo para mostrar por pantalla
		/* FIN PRUEBAS SPARK SQL */

		// typedTrainingData.write().format("com.databricks.spark.csv").option("header",
		// "true").save("output/consulta.csv");

		// Comprobación de Parser de ficheros CSV
		/*
		 * 
		 * CSVParser cpars = new CSVParser(); cpars.main();
		 */

		// NnnnnnUevo INTENTO
		// cat *.csv > final.csv
		/*
		 * System.out.println("----------INICIO PRUEBA CSV PARSER----------"); String
		 * command = "cat *.csv > final.csv";//cd output/CSVs/ && cat *.csv >
		 * final.csv"; //for f in source1/* ; do fname="$(basename -- "$f")" ; //cat --
		 * "source1/$fname" "source2/$fname" > "output/$fname" ; done try { Process
		 * process = Runtime.getRuntime().exec(command);
		 * 
		 * BufferedReader reader = new BufferedReader( new
		 * InputStreamReader(process.getInputStream())); String line; while ((line =
		 * reader.readLine()) != null) { System.out.println(line); }
		 * 
		 * 
		 * 
		 * reader.close();
		 * 
		 * } catch (IOException e) { e.printStackTrace(); }
		 */
		// fin intento

		// System.out.println("----------FIN PRUEBA CSV PARSER----------");

		System.out.println("----------Algorithm MLSpark----------");

		// Combine multiple input columns to a Vector using Vector Assembler
		// utility
		// IMPORTANTE: los vectores NO leen STRINGS
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "lat", "long", "velViento", "dirViento", "Vel_Media", "Vel_Max" })
				.setOutputCol("features");

		// Fecha, lat, long, velViento, dirViento, Vel_Media, Vel_Max
		// pRUEBO CON finalss
		/*
		 * typedTrainingData.createOrReplaceTempView("VECTORS"); Dataset<Row> vec =
		 * sparkSession
		 * .sql("SELECT CAST(Satelite as int) Satelite, CAST(Fecha as int) Fecha, CAST(lat as float) lat, "
		 * +
		 * "CAST(long as float) long, CAST(velViento as float) velViento, CAST(dirViento as float) dirViento FROM VECTORS"
		 * ); // CAST(vel_Media // as
		 * 
		 * Dataset<Row> velMsaxAVG = sparkSession.sql(
		 * "SELECT Fecha, avg(velViento) AS Vel_Media, MAX(velViento) AS Vel_Max FROM TRAINING_DATA GROUP BY Fecha"
		 * );
		 */
		// typedTrainingData.withColumn("Satelite",
		// typedTrainingData.col("Satelite").cast(FloatType));//S))("Satelite",
		// typedTrainingData.col("Satelite").cast("Double"));
		// Dataset<Row> typeedTrainingData = typedTrainingData.withColumn("Satelite",
		// typedTrainingData.col("Satelite").cast("Integer"));
		// typeedTrainingData.na().replace(typeedTrainingData.columns,Map("" ->
		// 0)).show();

		/*
		 * String[] satss = {"Name"}; typeedTrainingData =
		 * typedTrainingData.na().fill("Satelite", satss);
		 * System.out.println("NO NULL VALUES"); typedTrainingData.show();
		 */

		finalss.createOrReplaceTempView("AlgorithmSpark");

		Dataset<Row> algSpark = sparkSession.sql("SELECT CAST(Fecha as int) Fecha, CAST(lat as float) lat, "
				+ "CAST(long as float) long, CAST(velViento as float) velViento, CAST(dirViento as float) dirViento, CAST(Vel_Media as float) Vel_Media, CAST(Vel_Max as float) Vel_Max FROM AlgorithmSpark");

		System.out.println("----------AlgSpark contiene esto: ----------");
		algSpark.show();
		// typeedTrainingData =
		// typeedTrainingData.na().replace(typeedTrainingData.col("Satelite"), Map(0 =
		// 51));//("Satelite", Seq<String>("Satelite"));
		// typeedTrainingData =
		// typeedTrainingData.withColumn(typeedTrainingData.col("Satelite"),
		// functions.lit(null));

		// s.withColumn(c, ds.col(c).cast("string"))
		// val analysisData = dataframe_mysql.withColumn("Event",
		// dataframe_mysql("Event").cast(DoubleType))

		// StringIndexer genderIndexer = new
		// StringIndexer().setInputCol("Satelite").setOutputCol("Satelite");
		final Dataset<Row> featuresData = vectorAssembler.transform(algSpark);
		// Print Schema to see column names, types and other metadata
		System.out.println("--------IMPRIMIR ESQUEMA TRAS LA TRANSFORMACIÓN CON EL VECTOR--------");
		featuresData.printSchema();

		// Indexing is done to improve the execution times as comparing indexes
		// is much cheaper than comparing strings/floats

		// Index labels, adding metadata to the label column (dirViento). Fit on
		// whole dataset to include all labels in index.
		final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("dirViento")
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
		System.out.println("Example records with Predicted dirViento as 0:");
		predictions.select("predictedOccupancy", "dirViento", "features")
				.where(predictions.col("predictedOccupancy").equalTo(0)).show(10);

		System.out.println("Example records with Predicted dirViento as 1:");
		predictions.select("predictedOccupancy", "dirViento", "features")
				.where(predictions.col("predictedOccupancy").equalTo(1)).show(10);

		// En este vuelvo el resultado a un fichero CSV
		System.out.println("Example records with In-correct predictions:");
		predictions.select("predictedOccupancy", "dirViento", "features")
				.where(predictions.col("predictedOccupancy").notEqual(predictions.col("dirViento"))).show();
				//para escribir se usa lo de abajo, solo interesa mostrar por lo que uso show
				/*.write()
				.format("com.databricks.spark.csv").option("header", "true").mode("overwrite")
				.save("output/algoritmo.csv");
*/
		// .show(10); con esto muestro

		// predictions.write().text(sal);

		System.out.println("----------End of a MLSpark Process Execution----------");

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
		pw.println("@ATTRIBUTE satelite numeric");
		// pw.println("@ATTRIBUTE fechaGregorian date \"yyyy-MM-dd\"");
		pw.println("@ATTRIBUTE fechaJuliana numeric");
		pw.println("@ATTRIBUTE latitud numeric");
		pw.println("@ATTRIBUTE longitud numeric");
		pw.println("@ATTRIBUTE dirección_viento numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMax numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMed numeric");
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

	/*
	 * public void deleteDirectory(String path) {
	 * 
	 * File file = new File(path); if(file.isDirectory()){ String[] childFiles =
	 * file.list();
	 * 
	 * if(childFiles == null) { //Directory is empty. Proceed for deletion
	 * //System.out.println(childFiles.toString()); file.delete(); } else {
	 * //Directory has other files. //Need to delete them first for (String
	 * childFilePath : childFiles) { System.out.println(childFilePath.toString());
	 * //recursive delete the files deleteDirectory(childFilePath); } if (childFiles
	 * == null) { file.delete(); } }
	 * 
	 * } else { //it is a simple file. Proceed for deletion
	 * System.out.println("Es archivo, se borra directamente"); file.delete(); }
	 * 
	 * }
	 */

	

	// MAIN para pruebas, cuando se ejecute el proyecto se quita el Main, para
	// pruebas con Main
	public static void main(String[] args) throws IOException {
		// Dtree d = new Dtree(); String
		// rutaOut = args[0];//"output/consultita.csv"; String rutaIn =
		// args[1];//"resources/dataProject.csv"; d.sparkProcess(rutaIn,rutaOut); }

		String in = "/home/shadoop/Desktop/pruebasCSVJOse/"; // donde se carga el CSV
		/// /home/shadoop/Desktop/pruebasCSVJOse/
		// resources/*.csv
		String out = "output/CSVs"; // donde se generan los arff
		String conf = "output/conf/Configuracion2.txt"; // fichero configuración
		Dtree2 d = new Dtree2();
		d.sparkProcess(in, out, conf);
	}
}