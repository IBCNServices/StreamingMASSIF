package idlab.massif.sources;

import static spark.Spark.get;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.json.JSONArray;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;
import spark.Response;

import static spark.Spark.post;

class HTTPHandlerSingleton {
	private static HTTPHandlerSingleton single_instance = null;

	private Map<String, List<ListenerInf>> listenerMap;

	// private constructor restricted to this class itself
	private HTTPHandlerSingleton() {
		post("/httppostsource/:id", (req, res) -> prepData(req.params("id"), req.body(), res));
		listenerMap = new HashMap<String, List<ListenerInf>>();
	}

	// static method to create instance of Singleton class
	public static HTTPHandlerSingleton getInstance() {
		if (single_instance == null)
			single_instance = new HTTPHandlerSingleton();

		return single_instance;
	}

	private String prepData(String id, String body, Response response) {
		if (listenerMap.containsKey(id)) {
			listenerMap.get(id).forEach(l -> l.notify(0, body));
			response.status(200);
			return "found";
		} else {
			response.status(200);
			return "not found";
		}
	}


	public void removeRoute(String id) {
		listenerMap.remove(id);
	}

	public boolean addListener(String id, ListenerInf listener) {
		if (!listenerMap.containsKey(id)) {
			listenerMap.put(id,new ArrayList<ListenerInf>());
		}
		listenerMap.get(id).add(listener);
		return true;
	}
}

public class HTTPPostCombinedSource implements SourceInf {
	private String path;
	private HTTPHandlerSingleton handler;

	public HTTPPostCombinedSource(String path) {
		handler = HTTPHandlerSingleton.getInstance();
		this.path = path;
		

	}

	@Override
	public boolean addEvent(String event) {

		return false;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		// TODO Auto-generated method stub
		handler.addListener(path, listener);
		return true;
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
		return String.format("{\"type\":\"Source\",\"impl\":\"httpPostCombinedSource\",\"path\":\"%s\"}",
				path);
	}

}
