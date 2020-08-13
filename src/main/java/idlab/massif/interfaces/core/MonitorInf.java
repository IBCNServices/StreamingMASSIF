package idlab.massif.interfaces.core;

public interface MonitorInf {
	
	public long getQueueSize();
	public long getEventsIn();
	public long getEventsOut();
	public float getThroughput();
	

}
