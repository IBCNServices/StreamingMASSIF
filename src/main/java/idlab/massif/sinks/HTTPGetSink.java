package idlab.massif.sinks;

import static spark.Spark.get;

import java.util.ArrayList;
import java.util.List;

import org.json.JSONArray;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SinkInf;
import spark.route.Routes;

public class HTTPGetSink implements SinkInf{
	private List<String> lastEvents;
	private String config ="last";
	private String path;
	
	public HTTPGetSink(String path) {
		lastEvents = new ArrayList<String>();
		get("/"+path, (req, res) -> prepData());
		this.path = path;
	}
	public HTTPGetSink(String path,String config) {
		this(path);
		if(!config.isEmpty()) {
			this.config = config;
		}
	}
	
	private String prepData() {
		String jsonString = new JSONArray(lastEvents).toString();
		lastEvents.clear();
		return jsonString;
	}
	@Override
	public boolean addEvent(String event) {
		if(config.equals("last")) {
			lastEvents.clear();
		}
		lastEvents.add( event);
		
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
		
	}

}
