package UI;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.EventQueue;
import java.awt.GridLayout;
import java.awt.Image;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.io.IOException;
import javax.swing.JFrame;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.border.EmptyBorder;

import MapReduce.CSVSpark;
import MapReduce.Main_MapReduce;
import MapReduce.Mongo;
import MapReduce.MongoSparkTFG;
import MapReduce.MongoSparkTFG2;
import MapReduce.ejemploKmeans;

import javax.imageio.ImageIO;
import javax.swing.ImageIcon;
import javax.swing.JButton;

/**
 * Esta clase es la encargada de generar la UI
 * 
 * @author: Jose Manuel Romero
 * @version: 01/10/2018
 */
public class MainWindow extends JFrame {

	private JPanel contentPane;

	/**
	 * Launch the application.
	 */
	public static void main(String[] args) {
		EventQueue.invokeLater(new Runnable() {
			public void run() {
				try {
					MainWindow frame = new MainWindow();
					frame.setVisible(true);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
		});
	}

	/**
	 * Create the frame.
	 */
	public MainWindow() {
		GridLayout experimentLayout = new GridLayout(0, 1);
		// JFrame f=new JFrame();
		setTitle("Proyecto de Investigación Fundación HUPA - JoséManuel");
		setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		setBounds(150, 150, 500, 350);
		setBackground(Color.BLUE);
		contentPane = new JPanel();
		// contentPane.setLayout(experimentLayout);

		contentPane.setBorder(new EmptyBorder(7, 7, 7, 7));
		contentPane.setLayout(new BorderLayout(0, 0));
		setContentPane(contentPane);

		final Rutas rutas = new Rutas();
		contentPane.add(rutas, BorderLayout.NORTH);

		// final SeleccionManual seleccionManual = new SeleccionManual();
		// contentPane.add(seleccionManual);

		JPanel jsm = new JPanel();

		/*
		 * JButton mongo = new JButton("Mongo"); mongo.setBackground(Color.RED);
		 * mongo.setForeground(Color.WHITE); jsm.add(mongo);
		 * 
		 * JButton btnIniciar = new JButton("Csv ");
		 * btnIniciar.setBackground(Color.WHITE); btnIniciar.setForeground(Color.RED);
		 * jsm.add(btnIniciar);
		 */

		// Aquí añado botón a SPARK

		JButton spark = new JButton("Decision Tree MLlib");

		// Simple K Means WEKA
		JButton simplekmeans = new JButton("Simple K Means");

		// Añadir imagen a botón
		/*
		 * try { Image img = ImageIO.read(getClass().getResource("sparkicon.bmp"));
		 * spark.setIcon(new ImageIcon(img)); } catch (Exception ex) {
		 * System.out.println(ex); }
		 */

		jsm.add(spark);
		spark.setBackground(Color.BLUE);
		spark.setForeground(Color.WHITE);

		jsm.add(simplekmeans);
		simplekmeans.setBackground(Color.BLUE);
		simplekmeans.setForeground(Color.WHITE);
		// Botón SPark

		// Aquí añado botón a MongoSPARK
		/*
		 * JButton mongoSpark = new JButton("MongoSpark"); jsm.add(mongoSpark);
		 * mongoSpark.setBackground(Color.ORANGE);
		 * mongoSpark.setForeground(Color.WHITE);
		 */
		// Botón MongoSPark

		jsm.setLayout(experimentLayout);

		contentPane.add(jsm);

		contentPane.setLayout(new GridLayout(0, 1));
		contentPane.setSize(10, 10);

		// contentPane.setLayout(new GridLayout(0,1));

		/*
		 * btnIniciar.addActionListener(new ActionListener() { private String[] args;
		 * 
		 * @Override public void actionPerformed(ActionEvent arg0) { args = new
		 * String[3]; args[0] = rutas.getCsv(); args[1] = rutas.getSalida();
		 * Main_MapReduce main = new Main_MapReduce(); int resultado = 2;
		 * 
		 * try { if (!seleccionManual.seleccionado()) { // Archivo configuracion
		 * 
		 * args[2] = seleccionManual.getConf(); try { resultado = main.ejecutar(args); }
		 * catch (Exception e) {
		 * 
		 * e.printStackTrace(); }
		 * 
		 * } else { // Seleccion manual
		 * 
		 * seleccionManual.escribirFichero(); args[2] = "Configuracion.txt"; try {
		 * resultado = main.ejecutar(args); } catch (Exception e) { e.printStackTrace();
		 * }
		 * 
		 * }
		 * 
		 * String mensaje = null; if (resultado == 0) { mensaje =
		 * "Operación realizada con exito, mire el directorio de salida"; } else if
		 * (resultado == 1) { mensaje =
		 * "Se ha producido algún error durante la ejecución"; }
		 * JOptionPane.showMessageDialog(null, mensaje);
		 * 
		 * } catch (NumberFormatException | IOException e) {
		 * 
		 * e.printStackTrace(); }
		 * 
		 * } }); mongo.addActionListener(new ActionListener() {
		 * 
		 * @Override public void actionPerformed(ActionEvent arg0) {
		 * 
		 * Mongo main = new Mongo(); int resultado = 2;
		 * 
		 * try { if (!seleccionManual.seleccionado()) { // Archivo configuracion
		 * 
		 * try { main.map(seleccionManual.getConf(), rutas.getSalida()); resultado = 0;
		 * } catch (Exception e) { resultado = 1; e.printStackTrace(); }
		 * 
		 * } else { // Seleccion manual
		 * 
		 * seleccionManual.escribirFichero();
		 * 
		 * try { main.map("Configuracion.txt", rutas.getSalida()); resultado = 0; }
		 * catch (Exception e) { e.printStackTrace(); resultado = 1; }
		 * 
		 * }
		 * 
		 * String mensaje = null; if (resultado == 0) { mensaje =
		 * "Operación realizada con exito, mire el directorio de salida"; } else if
		 * (resultado == 1) { mensaje =
		 * "Se ha producido algún error durante la ejecución"; }
		 * JOptionPane.showMessageDialog(null, mensaje);
		 * 
		 * } catch (NumberFormatException | IOException e) {
		 * 
		 * e.printStackTrace(); }
		 * 
		 * } });
		 */

		// AL PULSAR ACCIÓN SOBRE Spark
		spark.addActionListener(new ActionListener() {
			private String[] args;

			@Override
			public void actionPerformed(ActionEvent arg0) {
				String mensaje = "Error!";
				int resultado = 2;
				args = new String[3];
				args[0] = rutas.getCsv();
				args[1] = rutas.getSalida();
				int x = 1;

				CSVSpark main = new CSVSpark();

				/*
				 * try { //ESTO se pone si no quiero archivo de Configuración.txt
				 * main.sparkProcess(rutas.getCsv(), rutas.getSalida()); resultado = 0; } catch
				 * (Exception e) { e.printStackTrace(); resultado = 1; }
				 */

				try {
					// Archivo configuracion

					// args[2] = seleccionManual.getConf();
					try {
						main.sparkProcess(args[0], args[1]);// , args[2]
						resultado = 0;
					} catch (Exception e) {
						resultado = 1;
						e.printStackTrace();
					}

					if (resultado == 0) {
						mensaje = "Operación realizada con exito, mire el directorio de salida";
					} else if (resultado == 1) {
						mensaje = "Se ha producido algún error durante la ejecución";
					}
					JOptionPane.showMessageDialog(null, mensaje);

				} catch (NumberFormatException e) {

					e.printStackTrace();
				}

				/*
				 * 
				 * args = new String[3]; args[0] = rutas.getCsv(); args[1] = rutas.getSalida();
				 * Main_MapReduce main = new Main_MapReduce(); int resultado = 2;
				 * 
				 * try { if (!seleccionManual.seleccionado()) { // Archivo configuracion
				 * 
				 * args[2] = seleccionManual.getConf(); try { resultado = main.ejecutar(args); }
				 * catch (Exception e) {
				 * 
				 * e.printStackTrace(); }
				 * 
				 * } else { // Seleccion manual
				 * 
				 * seleccionManual.escribirFichero(); args[2] = "Configuracion.txt"; try {
				 * resultado = main.ejecutar(args); } catch (Exception e) { e.printStackTrace();
				 * }
				 * 
				 * }
				 * 
				 * String mensaje = null; if (resultado == 0) { mensaje =
				 * "Operación realizada con exito, mire el directorio de salida"; } else if
				 * (resultado == 1) { mensaje =
				 * "Se ha producido algún error durante la ejecución"; }
				 * JOptionPane.showMessageDialog(null, mensaje);
				 * 
				 * } catch (NumberFormatException | IOException e) {
				 * 
				 * e.printStackTrace(); }
				 * 
				 * }
				 * 
				 */

				/*
				 * int resultado = 2; try {
				 * 
				 * if (!seleccionManual.seleccionado()) { // Archivo configuracion
				 * 
				 * try { main.sparkProcess(); //rutas.getSalida() resultado = 0; } catch
				 * (Exception e) { resultado = 1; e.printStackTrace(); }
				 * 
				 * } else { // Seleccion manual
				 * 
				 * seleccionManual.escribirFichero();
				 * 
				 * try { JOptionPane.showMessageDialog(null, mensaje);
				 * 
				 * } catch (Exception e) { e.printStackTrace(); resultado = 1; }
				 * 
				 * }
				 * 
				 * if (resultado == 0) { mensaje =
				 * "Operación realizada con exito, mire el directorio de salida"; } else if
				 * (resultado == 1) { mensaje =
				 * "Se ha producido algún error durante la ejecución"; }
				 * JOptionPane.showMessageDialog(null, mensaje);
				 * 
				 * } catch (NumberFormatException | IOException e) {
				 * 
				 * e.printStackTrace(); }
				 */// FIN PRUEBAS
				/*
				 * String mensaje = "Hola Jose"; JOptionPane.showMessageDialog(null, mensaje);
				 */

				/*
				 * Mongo main = new Mongo(); int resultado = 2;
				 * 
				 * try { if (!seleccionManual.seleccionado()) { // Archivo configuracion
				 * 
				 * try { main.map(seleccionManual.getConf(), rutas.getSalida()); resultado = 0;
				 * } catch (Exception e) { resultado = 1; e.printStackTrace(); }
				 * 
				 * } else { // Seleccion manual
				 * 
				 * seleccionManual.escribirFichero();
				 * 
				 * try { main.map("Configuracion.txt", rutas.getSalida()); resultado = 0; }
				 * catch (Exception e) { e.printStackTrace(); resultado = 1; }
				 * 
				 * }
				 * 
				 * String mensaje = null; if (resultado == 0) { mensaje =
				 * "Operación realizada con exito, mire el directorio de salida"; } else if
				 * (resultado == 1) { mensaje =
				 * "Se ha producido algún error durante la ejecución"; }
				 * JOptionPane.showMessageDialog(null, mensaje);
				 * 
				 * } catch (NumberFormatException | IOException e) {
				 * 
				 * e.printStackTrace(); }
				 */
			}
		});

		// AL PULSAR ACCIÓN SOBRE MongoSpark
		/*
		 * mongoSpark.addActionListener(new ActionListener() { private String[] args;
		 * 
		 * @Override public void actionPerformed(ActionEvent arg0) { String mensaje =
		 * "errooooooooor"; int resultado = 2; args = new String[3]; args[0] =
		 * rutas.getCsv(); args[1] = rutas.getSalida();
		 * 
		 * MongoSparkTFG2 main = new MongoSparkTFG2();
		 * 
		 * 
		 * 
		 * try { if (!seleccionManual.seleccionado()) { // Archivo configuracion
		 * 
		 * args[2] = seleccionManual.getConf(); try { main.mongoSparkExe(args[0],
		 * args[1], args[2]); resultado = 0; } catch (Exception e) { resultado = 1;
		 * e.printStackTrace(); }
		 * 
		 * } else { // Seleccion manual
		 * 
		 * seleccionManual.escribirFichero(); args[2] = "Configuracion.txt"; try {
		 * main.mongoSparkExe(args[0], args[1], args[2]); resultado = 0; } catch
		 * (Exception e) { resultado = 1; e.printStackTrace(); }
		 * 
		 * }
		 * 
		 * if (resultado == 0) { mensaje =
		 * "Operación realizada con exito, mire el directorio de salida"; } else if
		 * (resultado == 1) { mensaje =
		 * "Se ha producido algún error durante la ejecución"; }
		 * JOptionPane.showMessageDialog(null, mensaje);
		 * 
		 * } catch (NumberFormatException | IOException e) {
		 * 
		 * e.printStackTrace(); } } });
		 */

		/*
		 * try { //ESTO se pone si no quiero archivo de Configuración.txt
		 * main.sparkProcess(rutas.getCsv(), rutas.getSalida()); resultado = 0; } catch
		 * (Exception e) { e.printStackTrace(); resultado = 1; }
		 */

		simplekmeans.addActionListener(new ActionListener() {
			private String[] args;

			@Override
			public void actionPerformed(ActionEvent arg0) {
				String mensaje = "Error!";
				int resultado = 2;
				args = new String[3];
				args[0] = rutas.getCsv();
				args[1] = rutas.getSalida();
				int x = 1;

				ejemploKmeans main = new ejemploKmeans();

				/*
				 * try { //ESTO se pone si no quiero archivo de Configuración.txt
				 * main.sparkProcess(rutas.getCsv(), rutas.getSalida()); resultado = 0; } catch
				 * (Exception e) { e.printStackTrace(); resultado = 1; }
				 */

				try {
					// Archivo configuracion

					// args[2] = seleccionManual.getConf();
					try {
						main.kMeansSPark(args[0], args[1]);// , args[2]
						resultado = 0;
					} catch (Exception e) {
						resultado = 1;
						e.printStackTrace();
					}

					if (resultado == 0) {
						mensaje = "Operación realizada con exito, mire el directorio de salida";
					} else if (resultado == 1) {
						mensaje = "Se ha producido algún error durante la ejecución";
					}
					JOptionPane.showMessageDialog(null, mensaje);

				} catch (NumberFormatException e) {

					e.printStackTrace();
				}

			}

		});
	}
}
