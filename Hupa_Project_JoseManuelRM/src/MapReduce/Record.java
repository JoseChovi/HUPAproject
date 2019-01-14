package MapReduce;

import java.util.HashMap;
import java.util.Map;

/*CLASE CREADA PARA PARSEAR DIRECTORIO DE FICHEROS CSV*/
public class Record {
	private Map<String, String> values;

	public Record(String id) {
		this.values = new HashMap<String, String>();
	}

	public Map<String, String> getValues() {
		return values;
	}

	public void setValues(Map<String, String> values) {
		this.values = values;
	}

	public void put(String key, String value) {
		values.put(key, value);
	}

	public void get(String key) {
		values.get(key);
	}
}