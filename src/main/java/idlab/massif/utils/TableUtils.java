package idlab.massif.utils;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TableUtils {
	
	public static Map<String,List<String>> convertStringToTable(String event){
		Map<String,List<String>> table = new HashMap<String,List<String>>();
		String[] lines = event.split(System.lineSeparator());
		String[] vars = lines[0].split(",");
		List<String> keys = new ArrayList<String>();
		//Add the keys
		for(String var: vars) {
			if(!var.trim().equals("")) {
				table.put(var.trim(), new ArrayList<String>());
				keys.add(var.trim());
			}
		}
		
		//Add the data
		for(int lineCounter = 1; lineCounter < lines.length; lineCounter++) {
			String[] bindings = lines[lineCounter].split(",");
			for(int keyIndex = 0 ; keyIndex < keys.size(); keyIndex++) {
				table.get(keys.get(keyIndex).trim()).add(bindings[keyIndex].trim());
			}
			
		}
		return table;
	}

}
