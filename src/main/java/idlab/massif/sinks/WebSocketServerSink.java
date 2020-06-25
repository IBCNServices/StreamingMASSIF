package idlab.massif.sinks;

import idlab.massif.interfaces.core.ListenerInf;

import idlab.massif.interfaces.core.SinkInf;
import idlab.massif.sinks.utils.WebSocketOutputStream;
import spark.Service;
import spark.Spark;


public class WebSocketServerSink implements SinkInf{

	private WebSocketOutputStream socket;

	public WebSocketServerSink(int port, String wsURL) {
		Service ws = Service.ignite()
       		 .port(port);
		
		this.socket = new WebSocketOutputStream();
		ws.webSocket("/"+wsURL, socket);
		ws.init();
	}

	
	@Override
	public boolean addEvent(String event) {
		this.socket.notify(event);
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
		this.socket.close();
	}
}
