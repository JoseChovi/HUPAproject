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
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.ForeachFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.classification.DecisionTreeClassifier;
import org.apache.spark.ml.feature.IndexToString;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.VectorIndexer;
import org.apache.spark.ml.feature.VectorIndexerModel;
import org.apache.spark.mllib.clustering.KMeans;
import org.apache.spark.mllib.clustering.KMeansModel;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.catalyst.expressions.Coalesce;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.streaming.util.BatchedWriteAheadLog.Record;
import static org.apache.spark.sql.functions.split;
import org.apache.spark.sql.functions.*;

import scala.collection.Seq;

import org.apache.commons.io.FileUtils;

/**
 * 
 * @author Jose Manuel Romero Muelas Graduado en Ingeniería Informática en
 *         Ingeniería Software Proyecto desarrollado para la fundación HUPA 2018
 */
public class CSVSpark {

	/**
	 * @param args
	 * @throws IOException
	 */
	public void sparkProcess(String In, String out) throws IOException {
		// Create Spark Session to create connection to Spark
		final SparkSession sparkSession = SparkSession.builder().appName("Big Data Process Spark and CSV files")
				.master("local[*]").getOrCreate();

		// Añadido para eliminar mensajes de log en la consola
		sparkSession.sparkContext().setLogLevel("ERROR");
		// JavaSparkContext sc = new JavaSparkContext();

		final DataFrameReader dataFrameReader = sparkSession.read().option("header", true);
		final Dataset<Row> trainingData = dataFrameReader.csv(In + "/*.csv").toDF("IDPatient", "Institution",
				"Register", "ECG_EKG", "Temp", "SPO2Min", "SPO2Max", "BPMmin", "BPMmax", "BPMavg", "SYS", "DIA",
				"EDAmin", "EDAmax", "EDAavg", "ResultDeleted", "ResultUploaded", "DateUploaded", "DateDeleted");
		// con "*.csv" para que lea todo el directorio

		System.out.println("##### Rutas de los ficheros de entrada, salida y configuración ##### \n");
		System.out.println("Archivo de entrada: " + In + "\n");
		System.out.println("Directorio de salida: " + out + "\n");
		// System.out.println("Fichero de Configuración: " + conf + "\n");

		// Se crea una vista para obtener columnas y datos
		trainingData.createOrReplaceTempView("TRAINING_DATA");
		final Dataset<Row> typedTrainingData = sparkSession.sql(
				"SELECT CAST(IDPatient as String) IDPatient, CAST(Institution as String) Institution, CAST(Register as String) Register, "
						+ "CAST(ECG_EKG as String) ECG_EKG, CAST(Temp as float) Temp, CAST(SPO2Min as float) SPO2Min, "
						+ "CAST(SPO2Max as float) SPO2Max, CAST(BPMmin as float) BPMmin, CAST(BPMmax as float) BPMmax, CAST(BPMavg as float) BPMavg, "
						+ "CAST(SYS as float) SYS, CAST(DIA as float) DIA, CAST(EDAmin as float) EDAmin, CAST(EDAmax as float) EDAmax, CAST(EDAavg as float) EDAavg, "
						+ "CAST(ResultDeleted as String) ResultDeleted, CAST(ResultUploaded as String) ResultUploaded, CAST(DateUploaded as String) DateUploaded, CAST(DateDeleted as String) DateDeleted FROM TRAINING_DATA"); // CAST(vel_Media

		// typedTrainingData.show();
		// typedTrainingData.printSchema();
		System.out.println("Se pasa a ARRAY de Double los ECG/ECK");

		// Creo un nuevo esquema para los datos que voy a introducir
		StructType customStructType = new StructType();
		customStructType = customStructType.add("IDPatient", DataTypes.StringType, false);
		customStructType = customStructType.add("Institution", DataTypes.StringType, false);
		customStructType = customStructType.add("Register", DataTypes.StringType, false);
		customStructType = customStructType.add("ECG_EKG", DataTypes.createArrayType(DataTypes.DoubleType), false);
		customStructType = customStructType.add("Temp", DataTypes.FloatType, false);
		customStructType = customStructType.add("SPO2Min", DataTypes.FloatType, false);
		customStructType = customStructType.add("SPO2Max", DataTypes.FloatType, false);
		customStructType = customStructType.add("BPMmin", DataTypes.FloatType, false);
		customStructType = customStructType.add("BPMmax", DataTypes.FloatType, false);
		customStructType = customStructType.add("BPMavg", DataTypes.FloatType, false);
		customStructType = customStructType.add("SYS", DataTypes.FloatType, false);
		customStructType = customStructType.add("DIA", DataTypes.FloatType, false);
		customStructType = customStructType.add("EDAmin", DataTypes.FloatType, false);
		customStructType = customStructType.add("EDAmax", DataTypes.FloatType, false);
		customStructType = customStructType.add("EDAavg", DataTypes.FloatType, false);
		customStructType = customStructType.add("ResultDeleted", DataTypes.StringType, false);
		customStructType = customStructType.add("ResultUploaded", DataTypes.StringType, false);
		customStructType = customStructType.add("DateUploaded", DataTypes.StringType, false);
		customStructType = customStructType.add("DateDeleted", DataTypes.StringType, false);

		// y en este esquema adapto los datos leídos anteriormente

		Dataset<Row> newDF = typedTrainingData.map((MapFunction<Row, Row>) row -> {

			String strings[] = row.getString(3).split(", ");
			Double[] result = new Double[strings.length];
			for (int i = 0; i < strings.length; i++)
				result[i] = Double.parseDouble(strings[i]);
			// convert this to arry[] of type long

			return RowFactory.create(row.getString(0), row.getString(1), row.getString(2), result, row.getFloat(4),
					row.getFloat(5), row.getFloat(6), row.getFloat(7), row.getFloat(8), row.getFloat(9),
					row.getFloat(10), row.getFloat(11), row.getFloat(12), row.getFloat(13), row.getFloat(14),
					row.getString(15), row.getString(16), row.getString(17), row.getString(18));
		}, RowEncoder.apply(customStructType));

		// Ahora cargo los datos en el nuevo Dataset

		System.out.println("Se muestran los datos cargados desde los CSV con array de Double");
		newDF.show();
		newDF.printSchema();
		/*
		 * System.out.println("Fin Pruebo"); //ESTE SIRVE SOLO PARA OBTENER EL ARRAY
		 * Dataset<Row> ds1 =
		 * typedTrainingData.select(split(typedTrainingData.col("ECG_EKG"),
		 * ",")).toDF("ECG_EKG_f"); System.out.println("DS1 tiene esto");
		 * ds1.show(true); ds1.printSchema();
		 */

		System.out.println("##### Los diferentes pacientes cargados desde ficheros CSV son #####");
		newDF.select("IDPatient").distinct().show();
		
		System.out.println("Se omiten Strings como IDPatient, Institution, Register, Result Deleted, Result Uploaded, Data Uploaded y Data Deleted");

		// MACHINE LEARNING
		/*
		 * // Creación de Ficheros Weka File fdelete = new File("output/files1"); if
		 * (fdelete.exists()) { FileUtils.forceDelete(fdelete); }
		 * 
		 * // Creo un nuevo Dataset sin el string satelite para las pruebas de Weka
		 * //Dataset<Row> dasetWeka = sparkSession //
		 * .sql("SELECT Fecha, lat, long, velViento, dirViento, Vel_Media, Vel_Max FROM pruebasEjecucion"
		 * ); //System.out.
		 * println("##### Datos para el algoritmo Simple KMeans de Weka con un total de: "
		 * + newDF.count() + " #####");
		 * 
		 * newDF.javaRDD().map(Row -> Row.toString().replace("[", "").replace("]",
		 * "")).coalesce(1) .saveAsTextFile("output/files1");
		 * 
		 * System.out.println("##### Ejecución Algoritmo KMeans con Weka #####");
		 * 
		 * // Se abre ficheros de lectura y de escritura con los I/O String rutaDest =
		 * "output/wekafilesCSV/outWeka.arff"; FileWriter ficheroMax = null; try {
		 * ficheroMax = new FileWriter(rutaDest); } catch (IOException e) { // TODO
		 * Auto-generated catch block e.printStackTrace(); } PrintWriter pwMax = new
		 * PrintWriter(ficheroMax);
		 * 
		 * // Se inserta cabecera en el archivo .arff insertHeadARFF(pwMax);
		 * 
		 * // Aquí debo de leer el fichero txt y volcarlo en pwMax. FileReader fr = new
		 * FileReader("output/files1/part-00000");
		 * 
		 * String line;
		 * 
		 * try {
		 * 
		 * BufferedReader bufferreader = new BufferedReader(new
		 * FileReader("output/files1/part-00000"));
		 * 
		 * while ((line = bufferreader.readLine()) != null) { pwMax.println(line); }
		 * 
		 * } catch (FileNotFoundException ex) { ex.printStackTrace(); } catch
		 * (IOException ex) { ex.printStackTrace(); }
		 * 
		 * try
		 * 
		 * { ficheroMax.close(); } catch (IOException e) { // TODO Auto-generated catch
		 * block e.printStackTrace(); }
		 * 
		 * // Ejecución de Weka runWeka(
		 * "/home/shadoop/eclipse-workspace/Hupa_Project_JoseManuelRM/output/wekafilesCSV/",
		 * out); // outWeka.arff
		 * 
		 * System.out.println("##### Fin Ejecución Algortimo KMeans de Weka #####");
		 */
		// SIMPLE K MEANS
		/*
		 * 
		 * String path = ""; JavaRDD<String> data = sc.textFile(path); JavaRDD<Vector>
		 * parsedData = data.map(s -> { String[] sarray = s.split(","); double[] values
		 * = new double[sarray.length]; for (int i = 6; i < sarray.length; i++) {
		 * values[i] = Double.parseDouble(sarray[i]); } return Vectors.dense(values);
		 * }); parsedData.cache();
		 * 
		 * // Cluster the data into two classes using KMeans int numClusters = 2; int
		 * numIterations = 20; KMeansModel clusters = KMeans.train(parsedData.rdd(),
		 * numClusters, numIterations);
		 * 
		 * System.out.println("Cluster centers:"); for (Vector center :
		 * clusters.clusterCenters()) { System.out.println(" " + center); } double cost
		 * = clusters.computeCost(parsedData.rdd()); System.out.println("Cost: " +
		 * cost);
		 * 
		 * // Evaluate clustering by computing Within Set Sum of Squared Errors double
		 * WSSSE = clusters.computeCost(parsedData.rdd());
		 * System.out.println("Within Set Sum of Squared Errors = " + WSSSE);
		 * 
		 * // Save and load model clusters.save(jsc.sc(),
		 * "/home/shadoop/Desktop/pruebasCVSJoseM/KMeansModel"); KMeansModel sameModel =
		 * KMeansModel.load(jsc.sc(),
		 * "/home/shadoop/Desktop/pruebasCVSJoseM/KMeansModel");
		 * 
		 * // target/org/apache/spark/JavaKMeansExample/KMeansModel // $example off$
		 * 
		 * jsc.stop();
		 * 
		 * // FIN SIMPLE K MEANS
		 */
		System.out.println("##### Algoritmo de DecisionTree de Apache Spark MLlib #####");

		// Se combinan múltiples columnas de entrada en un vector usando VectorAssembler
		// IMPORTANTE: los vectores NO leen STRINGS

		// VECTORASSEMBLER, se indica las columnas de entrada y en la columna features
		// estarán todos los datos en una sola línea
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "Temp", "SPO2Min", "SPO2Max", "BPMmin", "BPMmax",
						"BPMavg", "SYS", "DIA", "EDAmin", "EDAmax", "EDAavg" })
				.setOutputCol("features");

		// Atributos quitados porque son String: "IDPatient", "Institution", "Register",
		// "ECG_EKG" , "ResultDeleted", "ResultUploaded", "DateUploaded", "DateDeleted"

		System.out.println("##### El Dataset que se vectoriza es el siguiente #####");
		newDF.show();

		newDF.createOrReplaceTempView("tdd");
		// Obtengo el dataset sin String ni Arrays
		Dataset<Row> transformedDS = sparkSession.sql(
				"SELECT CAST(Temp as float) Temp, CAST(SPO2Min as float) SPO2Min, "
						+ "CAST(SPO2Max as float) SPO2Max, CAST(BPMmin as float) BPMmin, CAST(BPMmax as float) BPMmax, CAST(BPMavg as float) BPMavg, "
						+ "CAST(SYS as float) SYS, CAST(DIA as float) DIA, CAST(EDAmin as float) EDAmin, CAST(EDAmax as float) EDAmax, CAST(EDAavg as float) EDAavg "
						+ "FROM tdd");

		System.out.println("##### El dataset transformado contiene los siguientes datos: ");
		transformedDS.show();

		// StringIndexer para transformar los Strings a números (ESTE FUNCIONA)
		//StringIndexerModel indexer = new StringIndexer().setInputCol("IDPatient").setOutputCol("IDPatient_index").fit(transformedDS);
		
		//indexer = new StringIndexer().setInputCol("Institution").setOutputCol("Institution_index").fit(transformedDS);
		

		// Aquí Gestionar los demás Strings

		/*
		 * IndexToString labelConverter = new IndexToString() .setInputCol("prediction")
		 * .setOutputCol("predictedLabel") .setLabels(labelIndexer.labels());
		 */

		// Nuevo dataset con el StringIndexer (ESTE FUNCIONA)
		//Dataset<Row> indexed = indexer.transform(transformedDS);
		
		/*
		//ONE HOT ENCODER
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
				  .setInputCols(new String[] {"IDPatient_index"})
				  .setOutputCols(new String[] {"categoryVec2"});

				OneHotEncoderModel modelHE = encoder.fit(indexed);
				Dataset<Row> encoded = modelHE.transform(indexed);
				System.out.println("Muestro el HOT ENCODER: ");
				encoded.show();
				System.out.println("fin HOT ENCODER: ");*/
		
		
		// Creo una vista para obtener los datos del dataset (ESTE FUNCIONA)
		//indexed.createOrReplaceTempView("tdd");
		// se obtienen los datos del indexer y se evitan los strings

		// Se trabaja con este dataset
		Dataset<Row> p1 = sparkSession.sql(
				"SELECT Temp, SPO2Min, SPO2Max, BPMmin, BPMmax, BPMavg, SYS, DIA, EDAmin, EDAmax, EDAavg FROM tdd");

		//System.out.println("Se comprueban los StringIndexer");
		p1.show();

		final Dataset<Row> featuresData = vectorAssembler.transform(p1);
		// Print Schema para ver los nombres de las columnas, tipos y otros datos
		System.out.println("##### Esquema y datos tras vectorizar los datos del Dataset #####");
		featuresData.printSchema();
		featuresData.show();

		// La indexación se realiza para mejorar los tiempos de ejecución, ya que
		// comparar índices es mucho más barato que comparar String / Float

		// Indexe las etiquetas, agregando metadatos a la columna de la etiqueta
		// (dirViento). Ajuste en el conjunto de datos completo para incluir todas las
		// etiquetas en el índice.

		// Se añade metadata a la columna (wind_Speed). Se ajusta
		// todo el dataset incluyendo todas las etiquetas en el índice
		final StringIndexerModel labelIndexer = new StringIndexer().setInputCol("Temp").setOutputCol("indexedLabel")
				.fit(featuresData);
		

		// Índice de vectores de características
		final VectorIndexerModel featureIndexer = new VectorIndexer().setInputCol("features")
				.setOutputCol("indexedFeatures").fit(featuresData);

		// Se dividen los datos en conjuntos de entrenamiento y pruebas (el 30% para las
		// pruebas).
		Dataset<Row>[] splits = featuresData.randomSplit(new double[] { 0.7, 0.3 });
		Dataset<Row> trainingFeaturesData = splits[0];
		Dataset<Row> testFeaturesData = splits[1];

		// Train a DecisionTree model.
		final DecisionTreeClassifier dt = new DecisionTreeClassifier().setLabelCol("indexedLabel")
				.setFeaturesCol("indexedFeatures");

		// Convertir etiquetas indexadas de nuevo a etiquetas originales
		final IndexToString labelConverter = new IndexToString().setInputCol("prediction")
				.setOutputCol("predictedOccupancy").setLabels(labelIndexer.labels());

		//(ESTE FUNCIONA)
		//IndexToString back2string = new IndexToString().setInputCol("IDPatient_index").setOutputCol("IDPatientfin")
			//	.setLabels(indexer.labels());
		//IndexToString back2Institution = new IndexToString().setInputCol("Institution_index").setOutputCol("InstitutionFin")
		//		.setLabels(indexer.labels());
	
		
		// Encadenar indexadores y árbol en una pipeline.
		final Pipeline pipeline = new Pipeline()
				.setStages(new PipelineStage[] { labelIndexer, featureIndexer, dt, labelConverter });

		
		
		// Se prueba y los indexadores también
		final PipelineModel model = pipeline.fit(trainingFeaturesData);

		
		
		// Se hacen predicciones.
		final Dataset<Row> predictions = model.transform(testFeaturesData);
		
		System.out.println("Compruebo el back a los strings");
		predictions.show();


		// Se seleccionan las filas para el ejemplo.
		System.out.println("Example records with Predicted Temp as 0:");
		predictions.select("predictedOccupancy", "Temp", "features")
				.where(predictions.col("predictedOccupancy").equalTo(36.31)).show(10);

		System.out.println("Example records with Predicted Temp as 1:");
		predictions.select("predictedOccupancy", "Temp", "features")
				.where(predictions.col("predictedOccupancy").equalTo(36.3)).show(10);

		// En este vuelvo el resultado a un fichero CSV
		System.out.println("Example records with In-correct predictions:");
		predictions.select("predictedOccupancy", "Temp", "features")
				.where(predictions.col("predictedOccupancy").notEqual(predictions.col("Temp"))).show();
		// para escribir se usa lo de abajo, solo interesa mostrar por lo que uso show

		//

		System.out.println("#### Fin de la ejecución del Algortimo Decision Tree de MLlib Spark #####");

		//


	}

	/**
	 * Inserta la cabecera en los ficheros arff.
	 *
	 * @param pw es el printwrite sobre el que se inserta la cabecera
	 */
	private void insertHeadARFF(PrintWriter pw) {
		pw.println("% ARFF");
		pw.println("% Fichero para Weka con los datos del RS de 2015");

		pw.println("@RELATION rapidscat_ascat");
		// pw.println("@ATTRIBUTE fechaGregorian date \"yyyy-MM-dd\"");
		pw.println("@ATTRIBUTE fechaJuliana numeric");
		// pw.println("@ATTRIBUTE satelite string");
		pw.println("@ATTRIBUTE latitud numeric");
		pw.println("@ATTRIBUTE longitud numeric");
		pw.println("@ATTRIBUTE velocidad_viento numeric");
		pw.println("@ATTRIBUTE dirección_viento numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMax numeric");
		pw.println("@ATTRIBUTE velocidad_vientoMed numeric");
		pw.println("@DATA");
	}

	// Ejecución de Weka
	private static void runWeka(String rutaFuente, String out) {

		// El siguiente comando es para LinearRegression
		// String command = "java -classpath
		// $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar weka.Run WekaForecaster -W
		// \"weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8
		// -num-decimal-places 4\" -t "
		// + rutaFuente + "*.arff " + " -F velocidad_vientoMax -L 1 -M 2 -G fechaJuliana
		// -prime 2";

		String command = "java -classpath $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar  weka.clusterers.SimpleKMeans -N 10 -t "
				+ rutaFuente + "*.arff";

		String ruta = rutaFuente.substring(0, rutaFuente.length() - 5);
		System.out.println("La ruta fuente es: " + rutaFuente);
		String rutaFinal = out;// "output/wekafiles.txt";//ruta + ".txt";
		FileWriter fichero = null;
		try {
			fichero = new FileWriter(rutaFinal + "/wekafilesFromCSV.txt");
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

	// MAIN para pruebas, cuando se ejecute el proyecto se quita el Main, para
	// pruebas con Main
	public static void main(String[] args) throws IOException {
		// Dtree d = new Dtree(); String
		// rutaOut = args[0];//"output/consultita.csv"; String rutaIn =
		// args[1];//"resources/dataProject.csv"; d.sparkProcess(rutaIn,rutaOut); }

		String in = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/"; // donde se carga el CSV
		/// /home/shadoop/Desktop/pruebasCSVJOse/
		// resources/*.csv
		String out = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/"; // donde se generan los arff
		// String conf = "output/conf/Configuracion2.txt"; // fichero configuración
		CSVSpark d = new CSVSpark();
		d.sparkProcess(in, out);
	}
}