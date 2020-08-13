package idlab.massif.core;

import static spark.Spark.get;


import java.util.HashMap;
import java.util.Map;


import org.json.JSONObject;

import idlab.massif.interfaces.core.MonitorInf;

public class HTTPMonitoringSingleTon {

	private static HTTPMonitoringSingleTon single_instance = null;

	private Map<String, MonitorInf> eventSourceMap;

	// private constructor restricted to this class itself
	private HTTPMonitoringSingleTon() {
		get("/monitor/:id", (req, res) -> prepData(req.params("id")));
		eventSourceMap = new HashMap<String, MonitorInf>();
	}

	// static method to create instance of Singleton class
	public static HTTPMonitoringSingleTon getInstance() {
		if (single_instance == null)
			single_instance = new HTTPMonitoringSingleTon();

		return single_instance;
	}

	private String prepData(String id) {
		Map<String,Object> results = new HashMap<String,Object>();
		if (eventSourceMap.containsKey(id)) {
			results.put("queueSize", eventSourceMap.get(id).getQueueSize());
			results.put("eventsIn", eventSourceMap.get(id).getEventsIn());
			results.put("eventsOut", eventSourceMap.get(id).getEventsOut());
			results.put("throughput", eventSourceMap.get(id).getThroughput());
		}
		;
		return new JSONObject(results).toString();
	}

	public void registerMonitor(String id, MonitorInf monitor) {
		eventSourceMap.put(id, monitor);
	}

	public void removeMonitor(String id) {
		eventSourceMap.remove(id);
	}
}
