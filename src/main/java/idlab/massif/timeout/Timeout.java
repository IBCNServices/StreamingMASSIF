package idlab.massif.timeout;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.PipeLineElement;

public class Timeout implements PipeLineElement {

	private ListenerInf listener;
	private double timeout;
	private double prevMessage;

	public Timeout(String timeout) {
		this.timeout = Double.parseDouble(timeout);
		this.prevMessage = 0;

	}

	@Override
	public boolean addEvent(String event) {
		if (System.currentTimeMillis() - prevMessage > timeout) {
			listener.notify(0, event);
			prevMessage = System.currentTimeMillis();
		}
		
		return false;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
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
	@Override
	public String toString() {
		return String.format("{\"type\":\"TimeOut\",\"impl\":\"timeout\",\"option1\":%s}",timeout +"");

	}

}
