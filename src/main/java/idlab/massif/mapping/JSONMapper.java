package idlab.massif.mapping;

import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.jsoniter.JsonIterator;
import com.jsoniter.any.Any;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.MapperInf;

public class JSONMapper implements MapperInf {

	public static void main(String[] args) {
		String json = "[{\"tsReceivedMs\":1610032728503,\"metricId\":\"org.dyamand.types.common.Loudness::number\",\"timestamp\":1610032550000,\"sourceId\":\"70:ee:50:73:3d:a2\",\"geohash\":\"7zzzzzzzzzzz\",\"h3Index\":646078419604526808,\"elevation\":0.0,\"value\":32.0,\"tags\":{\"_scope\":\"idlab.igent\",\"_auth\":\"service-account-dyamand\"},\"obeliskDelayMs\":197},"
			+"{\"tsReceivedMs\":1610032728503,\"metricId\":\"org.dyamand.types.common.Loudness::number\",\"timestamp\":77,\"sourceId\":\"70:ee:50:73:3d:a2\",\"geohash\":\"7zzzzzzzzzzz\",\"h3Index\":646078419604526808,\"elevation\":0.0,\"value\":32.0,\"tags\":{\"_scope\":\"idlab.igent\",\"_auth\":\"service-account-dyamand\"},\"obeliskDelayMs\":197}]";
		String test = JsonIterator.deserialize(json).toString("tags", "_scope");
		String mapping = "@prefix ex: <http://test/>.\n" + "@prefix sosa: <http://test2/>.\n"
				+ "ex:sensor_{sourceId} a sosa:Sensor;  sosa:observes ex:{sourceId}; sosa:hasResult {value}; ex:hasScope ex:{tags._scope}; ex:hasItTest {test.array*}.";

//		json = "{\"datum\":{\"Event\":{\"uuid\":\"835EB7B8-CC6A-5940-A952-18469BBFA613\",\"sequence\":{\"long\":3},\"type\":\"EVENT_FCNTL\",\"threadId\":{\"int\":100106},\"hostId\":\"83C8ED1F-5045-DBCD-B39F-918F0DF4F851\",\"subject\":{\"UUID\":\"269A60A2-39BE-11E8-B8CE-15D78AC88FB6\"},\"predicateObject\":null,\"predicateObjectPath\":null,\"predicateObject2\":null,\"predicateObject2Path\":null,\"timestampNanos\":1523037672740266752,\"name\":{\"string\":\"aue_fcntl\"},\"parameters\":{\"array\":[{\"size\":-1,\"type\":\"VALUE_TYPE_CONTROL\",\"valueDataType\":\"VALUE_DATA_TYPE_INT\",\"isNull\":false,\"name\":{\"string\":\"cmd\"},\"runtimeDataType\":null,\"valueBytes\":{\"bytes\":\"03\"},\"provenance\":null,\"tag\":null,\"components\":null}]},\"location\":null,\"size\":null,\"programPoint\":null,\"properties\":{\"map\":{\"host\":\"83c8ed1f-5045-dbcd-b39f-918f0df4f851\",\"return_value\":\"2\",\"fd\":\"3\",\"exec\":\"python2.7\",\"ppid\":\"1\"}}}},\"CDMVersion\":\"18\",\"source\":\"SOURCE_FREEBSD_DTRACE_CADETS\"}\n"
//				+ "";

		mapping = "<https://obelisk.ilabt.imec.be/api/v2/scopes/{tags._scope}/things/{sourceId}/metrics/{metricId}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/ObservableProperty> .\n"
				+ "<https://obelisk.ilabt.imec.be/api/v2/sensors/{sourceId}> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/Sensor>;\n"
				+ "	<http://www.w3.org/ns/sosa/observes>  <https://obelisk.ilabt.imec.be/api/v2/scopes/{tags._scope}/things/{sourceId}/metrics/{metricId}>.\n"
				+ "<https://obelisk.ilabt.imec.be/api/v2/sensors/{sourceId}/{metricId}/events/{timestamp}> <http://www.w3.org/ns/sosa/hasSimpleResult> \"{value}\"^^<http://www.w3.org/2001/XMLSchema#float> ;\n"
				+ "		 <http://www.w3.org/ns/sosa/resultTime> \"{timestamp}\" ;\n"
				+ "		<http://www.w3.org/ns/sosa/madeBySensor> <https://obelisk.ilabt.imec.be/api/v2/sensors/{sourceId}> ;\n"
				+ "		<http://www.w3.org/ns/sosa/observedProperty> <https://obelisk.ilabt.imec.be/api/v2/scopes/{tags._scope}/things/{sourceId}/metrics/{metricId}> ;\n"
				+ "		<http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/Observation> .";

		JSONMapper mapper = new JSONMapper(mapping);
		System.out.println(json);
		System.out.println("mapping:");
		System.out.println(mapping);
		System.out.println("result");
		Set<String> variables = mapper.variables();
		System.out.println(mapper.map(json));
		System.out.println(variables);
		long time1 = System.currentTimeMillis();
		for (int i = 0; i < 1; i++) {
			mapper.map(json);
		}
		System.out.println(System.currentTimeMillis() - time1);
		System.out.println(mapper.parseJson(json));
	}

	private String mapping;
	private ListenerInf listener;

	public JSONMapper(String mapping) {
		this.mapping = mapping;
	}

	public Set<String> variables() {
		Set<String> vars = new HashSet<String>();
		char current = ' ';
		boolean active = false;
		int startIndex = 0;
		for (int c = 0; c < mapping.length(); c++) {
			char ch = mapping.charAt(c);
			if (ch == '{') {
				active = true;
				startIndex = c;
			} else if (ch == '}') {
				if (active == false) {
					System.out.println("Parsing error! Closing } found without starting {!");
				}
				vars.add(mapping.substring(startIndex + 1, c));
				active = false;
			}

		}
		return vars;
	}

	public List<String> getJsonValue(Any json, String var, Map<String, List<String>> bindings) {
		if (bindings.containsKey(var)) {
			return bindings.get(var);
		} else {
			List<String> bind = null;
			if (var.charAt(var.length() - 1) == '*') {
				// iterator found
				String query = var.substring(0, var.length() - 1);
				bind = new ArrayList<String>();
				for (Any n : json.get(query.split("\\."))) {
					bind.add(URLEncoder.encode(n.toString()));
				}
				bindings.put(query + "\\*", bind);
			} else {
				bind = new ArrayList<String>(1);
				bind.add(URLEncoder.encode(json.toString(var.split("\\."))));
				bindings.put(var, bind);
			}
			return bind;
		}
	}

	public Map<String, String> parseJson(String json) {
		Map<String, String> jsonMap = new HashMap<String, String>();
		boolean open = false;
		boolean key = false;
		boolean valueFound = false;
		int startIndex = 0;
		int stopIndex = 0;
		int startValueIndex = 0;
		String currentKey = "";
		char[] test = json.toCharArray();
		List<String> depthKeys = new ArrayList<String>();
		for (int c = 0; c < json.length(); c++) {
			char ch = test[c];
			if (ch == '{') {
				open = true;
				if (!currentKey.isEmpty()) {
					depthKeys.add(currentKey);
				}
				valueFound = false;

			} else if (ch == '}') {
				open = false;
				if (valueFound) {
					String currentValue = null;
					if (test[startValueIndex + 1] == '"' && test[c - 1] == '"') {
						currentValue = json.substring(startValueIndex + 2, c - 1);
					} else {
						currentValue = json.substring(startValueIndex + 1, c);
					}
					jsonMap.put(String.join(".", depthKeys) + "." + currentKey, currentValue);
					valueFound = false;
				}
				if (!depthKeys.isEmpty()) {
					depthKeys.remove(depthKeys.size() - 1);
				}

			} else if (ch == '"' && !valueFound) {
				if (!key) {
					// start of new key
					startIndex = c;
					key = true;
				} else {
					// new key found
					stopIndex = c;
					key = false;
					currentKey = json.substring(startIndex + 1, stopIndex);
				}

			} else if (ch == ':' && (test[c + 1] == '"' || test[c + 1] == '{' || test[c + 1] == '[')) {
				valueFound = true;
				startValueIndex = c;
			} else if (valueFound && ch == ',') {
				String currentValue = null;
				if (test[startValueIndex + 1] == '"' && test[c - 1] == '"') {
					currentValue = json.substring(startValueIndex + 2, c - 1);
				} else {
					currentValue = json.substring(startValueIndex + 1, c);
				}
				String newKey = currentKey;
				if (!depthKeys.isEmpty()) {
					newKey = String.join(".", depthKeys) + "." + currentKey;
				}
				jsonMap.put(newKey, currentValue);
				valueFound = false;
			} else if (valueFound && ch == ',') {

			}
		}
		return jsonMap;
	}

	public String map_fast(String input) {
		Map<String, String> bindings = parseJson(input);
		char current = ' ';
		boolean active = false;
		int startIndex = 0;
		int tripleIndex = 0;
		StringBuilder sb = new StringBuilder();
		for (int c = 0; c < mapping.length(); c++) {
			char ch = mapping.charAt(c);
			if (ch == '{') {
				active = true;
				startIndex = c;
				// append remainder fixed structure of the mapping file.
				sb.append(mapping, tripleIndex, startIndex);
			} else if (ch == '}') {
				if (active == false) {
					System.out.println("Parsing error! Closing } found without starting {!");
				}
				// find json value

				String query = mapping.substring(startIndex + 1, c);
				String bind = bindings.getOrDefault(query, "");
				sb.append(bind);
				active = false;
				tripleIndex = c + 1;
			}

		}
		sb.append(mapping, tripleIndex, mapping.length());

		return sb.toString();
	}
	private String map_helper(Any json) {
		Map<String, List<String>> bindings = new HashMap<String, List<String>>();

		char current = ' ';
		boolean active = false;
		int startIndex = 0;
		int tripleIndex = 0;
		StringBuilder sb = new StringBuilder();
		for (int c = 0; c < mapping.length(); c++) {
			char ch = mapping.charAt(c);
			if (ch == '{') {
				active = true;
				startIndex = c;
				// append remainder fixed structure of the mapping file.
				sb.append(mapping, tripleIndex, startIndex);
			} else if (ch == '}') {
				if (active == false) {
					System.out.println("Parsing error! Closing } found without starting {!");
				}
				// find json value

				String query = mapping.substring(startIndex + 1, c);
				List<String> bind = getJsonValue(json, query, bindings);
				sb.append(String.join(",", bind));
				active = false;
				tripleIndex = c + 1;
			}

		}
		sb.append(mapping, tripleIndex, mapping.length());

		return sb.toString();
	}
	public String map(String input) {
		StringBuilder sb = new StringBuilder();
		Any json = JsonIterator.deserialize(input);
		if(input.startsWith("[")) {
			
			for(Any j :json.asList()) {
				sb.append(map_helper(j)).append("\n");
			}
		}else {
			sb.append(map_helper(json));
		}
		return sb.toString();
	}

	public String map1(String input) {
		Set<String> vars = this.variables();
		Map<String, List<String>> bindings = new HashMap<String, List<String>>();
		Any json = JsonIterator.deserialize(input);
		// extract the bindings
		for (String var : vars) {
			if (var.charAt(var.length() - 1) == '*') {
				// iterator found
				String query = var.substring(0, var.length() - 1);
				List<String> bind = new ArrayList<String>();
				for (Any n : json.get(query.split("\\."))) {
					bind.add(n.toString());
				}
				bindings.put(query + "\\*", bind);
			} else {
				List<String> bind = new ArrayList<String>(1);
				bind.add(json.toString(var.split("\\.")));
				bindings.put(var, bind);
			}

		}
		String result = new String(mapping);
		for (Entry<String, List<String>> ent : bindings.entrySet()) {
			if (ent.getValue().size() == 1) {
				result = result.replaceAll("\\{" + ent.getKey() + "\\}", ent.getValue().get(0));
			} else {
				String listValues = String.join(",", ent.getValue());
				result = result.replaceAll("\\{" + ent.getKey() + "\\}", listValues);
			}
		}

		return result;
	}

	@Override
	public boolean addEvent(String event) {

		String mappedResult = this.map(event);
		if (listener != null) {
			listener.notify(0, mappedResult);
		}

		return false;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		// TODO Auto-generated method stub
		this.listener = listener;
		return true;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub

	}
	public String toString() {
		return String.format("{\"type\":\"mapper\",\"keepHeader\":%b,\"mapping\":\"%s\"}",false,mapping.replace("\"", "\\\"").replace("\n", "\\n").replace("\t", "\\t"));

	}

}
