package MapReduce;

import java.io.IOException;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
// $example on$
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.clustering.KMeans;
import org.apache.spark.ml.clustering.KMeansModel;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.linalg.Vectors;
import org.apache.spark.rdd.RDD;
import org.apache.spark.ml.Estimator;
import org.apache.spark.ml.clustering.BisectingKMeans;
import org.apache.spark.ml.clustering.BisectingKMeansModel;
import org.apache.spark.ml.evaluation.ClusteringEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.feature.OneHotEncoderEstimator;
import org.apache.spark.ml.feature.OneHotEncoderModel;
// $example off$
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

public class KMeansHotEncoder {
	public void kMeansSPark(String In, String out) {
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

		// MACHINE LEARNING
		
		newDF.createOrReplaceTempView("tdd");
		
		OneHotEncoderEstimator encoder = new OneHotEncoderEstimator()
				  .setInputCols(new String[] {"categoryIndex1", "categoryIndex2"})
				  .setOutputCols(new String[] {"categoryVec1", "categoryVec2"});

				OneHotEncoderModel model = encoder.fit(newDF);
				Dataset<Row> encoded = model.transform(newDF);
				encoded.show();
		
		
		// NO HOT ENCODER
/*
		final VectorAssembler vectorAssembler = new VectorAssembler()
				.setInputCols(new String[] { "IDPatient_index", "Temp", "SPO2Min", "SPO2Max", "BPMmin", "BPMmax",
						"BPMavg", "SYS", "DIA", "EDAmin", "EDAmax", "EDAavg" })
				.setOutputCol("features");
		
		Dataset<Row> transformedDS = sparkSession.sql(
				"SELECT CAST(IDPatient as String) IDPatient ,CAST(Temp as float) Temp, CAST(SPO2Min as float) SPO2Min, "
						+ "CAST(SPO2Max as float) SPO2Max, CAST(BPMmin as float) BPMmin, CAST(BPMmax as float) BPMmax, CAST(BPMavg as float) BPMavg, "
						+ "CAST(SYS as float) SYS, CAST(DIA as float) DIA, CAST(EDAmin as float) EDAmin, CAST(EDAmax as float) EDAmax, CAST(EDAavg as float) EDAavg "
						+ "FROM tdd");
		
		StringIndexerModel indexer = new StringIndexer().setInputCol("IDPatient").setOutputCol("IDPatient_index").fit(transformedDS);
		Dataset<Row> indexed = indexer.transform(transformedDS);
		indexed.createOrReplaceTempView("tdd");
		Dataset<Row> p1 = sparkSession.sql(
				"SELECT IDPatient_index, Temp, SPO2Min, SPO2Max, BPMmin, BPMmax, BPMavg, SYS, DIA, EDAmin, EDAmax, EDAavg FROM tdd");
		final Dataset<Row> featuresData = vectorAssembler.transform(p1);


		KMeans kmeans = new KMeans().setK(2).setSeed(1L);
		KMeansModel model = kmeans.fit(featuresData);
		// Make predictions
	    Dataset<Row> predictions = model.transform(featuresData);


	    // Evaluate clustering by computing Silhouette score
	    ClusteringEvaluator evaluator = new ClusteringEvaluator();

	    double silhouette = evaluator.evaluate(predictions);
	    System.out.println("Silhouette with squared euclidean distance = " + silhouette);

	    // Shows the result.
	    Vector[] centers = model.clusterCenters();
	    System.out.println("Cluster Centers: ");
	    for (Vector center: centers) {
	      System.out.println(center);
	    }
	    // $example off$
		
		/*
		int numClusters = 2;
		int numIterations = 20;
		KMeansModel clusters = KMeans.train(parsedData.rdd(), numClusters, numIterations);

		System.out.println("Cluster centers:");
		for (Vector center : clusters.clusterCenters()) {
			System.out.println(" " + center);
		}
		double cost = clusters.computeCost(parsedData.rdd());
		System.out.println("Cost: " + cost);

		// Evaluate clustering by computing Within Set Sum of Squared Errors
		double WSSSE = clusters.computeCost(parsedData.rdd());
		System.out.println("Within Set Sum of Squared Errors = " + WSSSE);

		// Save and load model
		clusters.save(jsc.sc(), "/home/shadoop/Desktop/pruebasCVSJoseM/KMeansModel");
		KMeansModel sameModel = KMeansModel.load(jsc.sc(), "/home/shadoop/Desktop/pruebasCVSJoseM/KMeansModel");

		// target/org/apache/spark/JavaKMeansExample/KMeansModel
		// $example off$

		jsc.stop();*/
	}
	
	public static void main(String[] args) throws IOException {
		// Dtree d = new Dtree(); String
		// rutaOut = args[0];//"output/consultita.csv"; String rutaIn =
		// args[1];//"resources/dataProject.csv"; d.sparkProcess(rutaIn,rutaOut); }

		String in = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/"; // donde se carga el CSV
		/// /home/shadoop/Desktop/pruebasCSVJOse/
		// resources/*.csv
		String out = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/"; // donde se generan los arff
		// String conf = "output/conf/Configuracion2.txt"; // fichero configuración
		KMeansHotEncoder d = new KMeansHotEncoder();
		d.kMeansSPark(in, out);
	}
	
	
}