package idlab.massif.sinks;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SinkInf;

public class PrintSink implements SinkInf{

	

	@Override
	public boolean addEvent(String event) {
		System.out.println("PrintSink received:");
		System.out.println(event);
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
	@Override
	public String toString() {
		return "{\"type\":\"Sink\",\"impl\":\"printSink\"}";
	}

}
