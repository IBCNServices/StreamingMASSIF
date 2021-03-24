package idlab.massif.sinks;

import static spark.Spark.get;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SinkInf;
import spark.route.Routes;

class HTTPHandlerSingleton {
	private static HTTPHandlerSingleton single_instance = null;

	private Map<String, List<String>> eventSourceMap;
	private Map<String, String> sourceConfigMap;

	// private constructor restricted to this class itself
	private HTTPHandlerSingleton() {
		get("/httpgetsink/:id", (req, res) -> prepData(req.params("id")));
		sourceConfigMap = new HashMap<String, String>();
		eventSourceMap = new HashMap<String, List<String>>();
	}

	// static method to create instance of Singleton class
	public static HTTPHandlerSingleton getInstance() {
		if (single_instance == null)
			single_instance = new HTTPHandlerSingleton();

		return single_instance;
	}

	private synchronized String prepData(String id) {
		List<String> lastEvents = null;
		if (eventSourceMap.containsKey(id)) {
			lastEvents = eventSourceMap.get(id);
		} else {
			lastEvents = Collections.emptyList();
		}
		String jsonString = new JSONArray(lastEvents).toString();
		lastEvents.clear();
		return jsonString;
	}

	public void registerRouter(String id) {
		eventSourceMap.put(id, new ArrayList<String>());
	}

	public void setConfig(String id, String config) {
		sourceConfigMap.put(id, config);
	}

	public synchronized void add(String id, String event) {
		if (eventSourceMap.containsKey(id)) {
			if (sourceConfigMap.get(id).equals("last")) {
				eventSourceMap.get(id).clear();
			}
			eventSourceMap.get(id).add(event);
		}
	}

	public void removeRoute(String id) {
		eventSourceMap.remove(id);
		sourceConfigMap.remove(id);
	}
}

public class HTTPGetCombinedSink implements SinkInf {
	private String path;
	private HTTPHandlerSingleton handler;
	private String config;
	public HTTPGetCombinedSink(String path) {
		handler = HTTPHandlerSingleton.getInstance();
		this.path = path;
		handler.registerRouter(path);
		handler.setConfig(path, "last");
		this.config ="";
	}

	public HTTPGetCombinedSink(String path, String config) {
		this(path);
		handler.setConfig(path, config);
		this.config = config;
	}

	@Override
	public boolean addEvent(String event) {
		handler.add(path, event);

		return true;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub

	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		handler.removeRoute(path);
	}
	@Override
	public String toString() {
		return String.format("{\"type\":\"Sink\",\"impl\":\"httpGetSinkCombined\",\"path\":\"%s\",\"config\":\"%s\"}",path,config);
	}

}
