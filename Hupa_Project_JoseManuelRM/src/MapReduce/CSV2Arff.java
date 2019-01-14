package MapReduce;

import weka.core.Instances;
import weka.core.converters.ConverterUtils.DataSource;
import weka.core.converters.ArffSaver;
import weka.core.converters.CSVLoader;
import weka.core.converters.Loader;

import java.io.File;

public class CSV2Arff {

	public static void main(String[] args) throws Exception {
		CSVLoader loader = new CSVLoader();
		loader.setSource(new File("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/pruebas.csv"));
		Instances data = loader.getDataSet();

		// save ARFF
		ArffSaver saver = new ArffSaver();
		saver.setInstances(data);
		// and save as ARFF
		saver.setFile(new File("/home/jromeroaj/Escritorio/Programas/HUPAdatos/PRUEBAS/fileprueba.arff"));
		saver.writeBatch();
	}
}
