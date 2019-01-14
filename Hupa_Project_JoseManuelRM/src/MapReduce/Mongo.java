package MapReduce;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;
import java.util.TreeMap;

import com.mongodb.BasicDBObject;

import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;



import org.bson.Document;
/**
 * Esta recorre la base de datos Nasa de Mongo y lanza weka por cada region.
 * @version: 10/1/2018
 * @author Diego Jose Merino Fernandez
 */
public class Mongo {
  private MongoClient mongoClient;
  private MongoDatabase db;
  private MongoCursor<Document> cursor;

  private List <TreeMap<Double, Punto>> ps2;
  public Mongo () {
    //conexion
    mongoClient = new MongoClient("localhost", 27017);
    //base de datos Nasa
    db = mongoClient.getDatabase("Nasa");
    //lista de treeMap uno por cada region introducida que tiene el valor maximo como primer parametro y el punto con el valor medio.
    ps2=new LinkedList<TreeMap<Double, Punto>>();
  
  }
  /**
   * Este metodo lee de mongo con la etiquetas satelite , para ir comprobando si los valores estan en alguna de las regiones introducidas.
   * Si es asi puede pasar dos cosas
   *    1º La el arbol de la region esta a null aun entonces se introduce como primer valor la velocidad del viento de este y como segundo la clase punto con los valores de
   *        la coleccion en la que estamos(tambien empezamos a calcular el valor medio).
   *    2º La region ya existe entonces vamos añadiendo su velocidad para sacar la media global.
   *
   * @param conf: ruta de la configuracion por defecto
   * @param sal: ruta de salida
   * @throws IOException Signals that an I/O exception has occurred.
   * @throws InterruptedException the interrupted exception
   */
 
    public void map(String conf,String sal)
      throws IOException, InterruptedException { 
      
    Double lat,lon;
  
    TreeMap<Double, Punto> tmapMax;
    
    MongoCollection<Document> collection = db.getCollection("satelites");
    BasicDBObject inQuery = new BasicDBObject();
    List<Integer> list = new ArrayList<Integer>();
    list.add(0);
    list.add(1);
    inQuery.put("Num", new BasicDBObject("$in", list));
    
    FindIterable<Document> find = collection.find(inQuery);
    List<Region> listaR = new ArrayList<Region>();

    BufferedReader br = new BufferedReader(new FileReader(conf));
    String line2 = br.readLine();
    while (line2 != null) {
      Region r = new Region();
      
      StringTokenizer st2 = new StringTokenizer(line2, ",");

      // Punto1
      r.setLatPunto1(Float.parseFloat((String) st2.nextElement()));
      r.setLonPunto1(Float.parseFloat((String) st2.nextElement()));
      // Punto2
      r.setLatPunto2(Float.parseFloat((String) st2.nextElement()));
      r.setLonPunto2(Float.parseFloat((String) st2.nextElement()));
      
      line2 = br.readLine();
      listaR.add(r);
    }
    for(int i=0; i<listaR.size();i++) {
      ps2.add(new TreeMap<Double, Punto>());
      Region d =listaR.get(i);
      System.out.println("region: "+d);
    }
    
    cursor = find.iterator();
    //Transformacion del value a punto
    while(cursor.hasNext()) {
      Document d=cursor.next();
      Punto p=new Punto();
      p.setLatitud(d.getDouble("latitud"));
      p.setLongitud(d.getDouble("longitud"));
      lat = p.getLatitud();
      lon = p.getLongitud();
   
 
    for (int i = 0; i < listaR.size(); i++) {
   
        
      if (lat <= listaR.get(i).getLatPunto1() & lat >= listaR.get(i).getLatPunto2()
          & lon <= listaR.get(i).getLonPunto1() & lon >= listaR.get(i).getLonPunto2()) {
        //System.out.println(lat+" "+lon);

        
        p.setSatelite(d.getInteger("Num"));
        p.setJulianaDay(Double.parseDouble(d.getString("Tiempo")));
       // System.out.println(d.getString("wind_Speed"));
        p.setVelocidad_viento(d.getDouble("wind_Speed"));
        p.setDireccion_viento(d.getDouble("wind_dir"));
        tmapMax=ps2.get(i);
        if(tmapMax.get(p.getJulianaDay()) == null){
          p.setVelocidad_viento_media(p.getVelocidad_viento());
          p.setnVelocidades(1);
          
          tmapMax.put(p.getJulianaDay(), p);
        }else{
          if(tmapMax.get(p.getJulianaDay()).getVelocidad_viento()<p.getVelocidad_viento()){
            //Actualiza la maxima
            tmapMax.get(p.getJulianaDay()).setVelocidad_viento(p.getVelocidad_viento());  
            tmapMax.get(p.getJulianaDay()).setDireccion_viento(p.getDireccion_viento()); 
          }
          //actualiza el contador y suma 1 a nVelocidades
          tmapMax.get(p.getJulianaDay()).incrementarVelocidadVientoMedia(p.getVelocidad_viento());
          
        }        

      }
    }
   
    }
    br.close();
    for(int i=0;i<ps2.size();i++) {
    tmapMax=ps2.get(i);
    
    Iterator it1 = tmapMax.keySet().iterator();
    while(it1.hasNext()){
      Double key1 =  (Double) it1.next();
      System.out.println(key1);
      System.out.println(tmapMax.get(key1).toString());
    
    }
    }
    if (!ps2.isEmpty())
    reduce(sal);
    

  
  }
    /**
     *Metodo que va recorriendo el arbol y creando los archivos .arff con los datos
     *
     * @param sal: ruta de salida
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws InterruptedException the interrupted exception
     */ 
  public void reduce(String sal) throws IOException, InterruptedException {
    TreeMap<Double, Punto> tmapMax;
    String rutaDest;
    for(int i=0;i<ps2.size();i++) {
    rutaDest= sal + "/Region" + i + ".arff";
    FileWriter ficheroMax = new FileWriter(rutaDest);  
    PrintWriter pwMax = new PrintWriter(ficheroMax); 
    tmapMax=ps2.get(i);
    insertHeadARFF(pwMax);
       
    //Recorre el arbol y lo imprime
    Iterator it1 = tmapMax.keySet().iterator();
    while(it1.hasNext()){
      Double key1 =  (Double) it1.next();
      pwMax.println(tmapMax.get(key1).toString());
    }
    ficheroMax.close();
    runWeka(rutaDest);

    }
 
    
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
    pw.println("@ATTRIBUTE fechaGregorian date \"yyyy-MM-dd\"");
    pw.println("@ATTRIBUTE fechaJuliana numeric");
    pw.println("@ATTRIBUTE latitud numeric");
    pw.println("@ATTRIBUTE longitud numeric");
    pw.println("@ATTRIBUTE dirección_viento numeric");
    pw.println("@ATTRIBUTE velocidad_vientoMax numeric");
    pw.println("@ATTRIBUTE velocidad_vientoMed numeric");
    pw.println("@DATA");
  }

  /**
   * Este metodo se encarga de llamar llamar a Weka y ejecutar el algoritmo
   * 
   * @param rutaFuente the ruta fuente
   */
  private static void runWeka(String rutaFuente) {

    String command = "java -classpath $CLASSPATH:/home/shadoop/weka-3-8-1/weka.jar weka.Run WekaForecaster -W \"weka.classifiers.functions.LinearRegression -S 0 -R 1.0E-8 -num-decimal-places 4\" -t " +rutaFuente + " -F velocidad_vientoMax -L 1 -M 2 -G fechaGregorian -prime 2";
    String ruta = rutaFuente.substring(0, rutaFuente.length() - 5);
    System.out.println(command);
    String rutaFinal = ruta + ".txt";
    FileWriter fichero = null;
    try {
      fichero = new FileWriter(rutaFinal);
      String output = executeCommand(command);
      System.out.println(output);
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

  /**
   * Ejecuta el comando
   *
   * @param command the command
   * @return the string
   */
  private static String executeCommand(String command) {

    StringBuffer output = new StringBuffer();

    Process p;
    try {
      p = Runtime.getRuntime().exec(new String[]{"bash","-c", command});
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
  
}
