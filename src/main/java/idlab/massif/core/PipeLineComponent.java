package idlab.massif.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicLong;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.MonitorInf;
import idlab.massif.interfaces.core.PipeLineElement;

public class PipeLineComponent implements ListenerInf, MonitorInf {

	private List<PipeLineComponent> output;
	Map<Integer,List<PipeLineComponent>> outputMap;
	private PipeLineElement element;
	private ThreadPoolExecutor queue = (ThreadPoolExecutor)Executors.newFixedThreadPool(1);
	private boolean isQueryComponentLinked = false;

	private AtomicLong inEvents= new AtomicLong(0);
	private AtomicLong outEvents= new AtomicLong(0);
	private long totalTime = 0;
	public PipeLineComponent(String id,PipeLineElement element, List<PipeLineComponent> output) {
		this(element,output);
		HTTPMonitoringSingleTon.getInstance().registerMonitor(id, this);
	}
	public PipeLineComponent(PipeLineElement element, List<PipeLineComponent> output) {
		this.output = output;
		this.element = element;
		element.addListener(this);

	}
	public PipeLineComponent(PipeLineElement element, Map<Integer,List<PipeLineComponent>> output) {
		this.outputMap = output;
		this.element = element;
		element.addListener(this);
		isQueryComponentLinked=true;

	}

	public void setOutput(List<PipeLineComponent> output) {
		this.output = output;
	}
	public void addEvent(String event) {
		inEvents.getAndIncrement();
		// add all arriving events to the queue
		queue.execute(new Runnable() {

			@Override
			public void run() {
				long time1 = System.currentTimeMillis();
				element.addEvent(event);
				totalTime+=(System.currentTimeMillis()-time1);

			}

		});
	}

	public void setQueryComponentLinked(boolean queryCompLinked) {
		this.isQueryComponentLinked = queryCompLinked;
	}

	@Override
	public void notify(int queryID, String event) {
		// send all the response from this component to all its output components
		outEvents.getAndIncrement();
		if (!isQueryComponentLinked) {
			for (PipeLineComponent comp : output) {
				comp.addEvent(event);
			}
		} else {
			for (PipeLineComponent comp : outputMap.get(queryID)) {
				comp.addEvent(event);
			}
		}
	}
	public PipeLineElement getElement() {
		return element;
	}
	@Override
	public long getQueueSize() {
		return queue.getQueue().size();
	}
	@Override
	public long getEventsIn() {
		return inEvents.get();
	}
	@Override
	public long getEventsOut() {
		return outEvents.get();
	}
	@Override
	public float getThroughput() {		
		return totalTime>0?(float)(((float)1000.0*outEvents.get())/(float)totalTime):0;
	}
	
		

}
