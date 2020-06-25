package idlab.massif.interfaces.core;

public interface PipeLineElement {
	public boolean addEvent(String event);
	public boolean addListener(ListenerInf listener);
	public void start();
	public void stop();

}
