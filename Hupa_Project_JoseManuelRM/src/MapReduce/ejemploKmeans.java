package MapReduce;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

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
// $example off$
import org.apache.spark.sql.DataFrameReader;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import weka.classifiers.trees.J48;
import weka.classifiers.trees.RandomForest;
//WEKA
import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.ConverterUtils;
import weka.core.converters.Loader;
import weka.datagenerators.Test;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import weka.classifiers.Classifier;
import weka.classifiers.Evaluation;
import weka.classifiers.evaluation.NominalPrediction;
import weka.classifiers.rules.DecisionTable;
import weka.classifiers.rules.PART;
import weka.classifiers.trees.DecisionStump;
import weka.classifiers.trees.J48;
import weka.core.FastVector;
import weka.core.Instances;
/*
 * Clase para la ejecución del algoritmo Simple K Means
 * a través del Framework WEKA.
 * Desarrollado por: José Manuel Romero
 * Fundación HUPA
 * */
public class ejemploKmeans {
	public void kMeansSPark(String In, String out) throws IOException {
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

		//Añado al método principal un Throw Exception en vez de varios try catch aquí
		//Se Genera la cabecera de ARFF File
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/pruebas.csv"));
		Instances data = loader.getDataSet();

		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		// and save as ARFF
		saver.setFile(new File("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/fileprueba.arff"));
		saver.writeBatch();
		
		//Ejecución del algoritmo Simple K Means a través de WEKA
		runWeka("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/", out); // outWeka.arff
		
		/*try {
			pruebasWeka("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/fileprueba.arff");
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}*/
		// MACHINE LEARNING
		
		
		//////////////////////////////////////
		

		// $example on$
		// Load and parse data
		/*RDD<Row> data = newDF.rdd();
		RDD<Vector> parsedData = data.map(s -> {
			String[] sarray = s.split(",");
			double[] values = new double[sarray.length];
			for (int i = 6; i < sarray.length; i++) {
				values[i] = Double.parseDouble(sarray[i]);
			}
			return Vectors.dense(values);
		});
		parsedData.cache();*/

		// Cluster the data into two classes using KMeans
		
		//quitar este para utilizar el de Spark MLlib
		/*newDF.createOrReplaceTempView("tdd");

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
	    }*/ //quitar este para utilizar el de Spark MLlib
		
		
		
	    // $example off$
		
		//ESTO ES DEL ANTERIOR NO SE ESTÁ UTILIZANDO
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
	
	// Ejecución de Weka
	private static void runWeka(String rutaFuente, String out) {

		// El siguiente comando es para LinearRegression
		// String command = "java -classpath
		// $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar weka.Run WekaForecaster -W
		// \"weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8
		// -num-decimal-places 4\" -t "
		// + rutaFuente + "*.arff " + " -F velocidad_vientoMax -L 1 -M 2 -G fechaJuliana
		// -prime 2";

		String command = "java -classpath $CLASSPATH:/home/jromeroaj/Escritorio/Programas/weka-3-8-3/weka.jar weka.clusterers.SimpleKMeans -N 5 -t "
				+ rutaFuente + "*.arff";
		
		//String command = "java -classpath $CLASSPATH:/home/jromeroaj/Escritorio/Programas/weka-3-8-3/weka.jar weka.classifiers.trees.J48 -C 0.25 -M 2 -X 3 -t "
		//		+ rutaFuente + "*.arff";
		
		//Para cmd java -classpath $CLASSPATH:/home/jromeroaj/Escritorio/Programas/weka-3-8-3/weka.jar weka.classifiers.trees.J48 -C 0.25 -M 2 -X 3 -t /home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/*.arff;
		

		//String command = "java -classpath $CLASSPATH:/home/jromeroaj/Escritorio/Programas/weka-3-8-3/weka.jar weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8 -num-decimal-places 4 -t "
		//		+ rutaFuente + "*.arff";
		
		//weka.clusterers.SimpleKMeans -N 5 -t
		//weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8 -num-decimal-places 4
		//weka.classifiers.functions.GaussianProcesses
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
	
	private static void pruebasWeka(String rutaFuente) throws Exception {
		
		// NO funciona ya que no son nominales los datos
		/*BufferedReader reader = new BufferedReader(new FileReader(rutaFuente));

		
        //Get the data
        Instances data = new Instances(reader);
        reader.close();
        
        //Setting class attribute 
        data.setClassIndex(data.numAttributes() - 1);
		
		J48 tree = new J48();
        String[] options = new String[1];
        options[0] = "-U"; 

        //Print tree
        System.out.println("Muestro el árbol del J48: " + tree + "\n Fin Árbol J48");
		*/

		//OTRO EJEMPLO
        /*try {
    		Classifier randomForest = new RandomForest();

    		ConverterUtils.DataSource source = new ConverterUtils.DataSource(rutaFuente);
    		Instances dataSet = source.getDataSet();

    		dataSet.setClassIndex(dataSet.numAttributes() - 1);
    		randomForest.buildClassifier(dataSet);

    		Classifier classifier = randomForest;
    	} catch (Exception e) {
    		e.printStackTrace();
    	}*/
        // HASTA AQUÍ
		
		
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
		// Dtree d = new Dtree(); String
		// rutaOut = args[0];//"output/consultita.csv"; String rutaIn =
		// args[1];//"resources/dataProject.csv"; d.sparkProcess(rutaIn,rutaOut); }

		String in = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/"; // donde se carga el CSV
		/// /home/shadoop/Desktop/pruebasCSVJOse/
		// resources/*.csv
		String out = "/home/jromeroaj/Escritorio/Programas/HUPAdatos/"; // donde se generan los arff
		// String conf = "output/conf/Configuracion2.txt"; // fichero configuración
		ejemploKmeans d = new ejemploKmeans();
		d.kMeansSPark(in, out);
	}
	
	
}