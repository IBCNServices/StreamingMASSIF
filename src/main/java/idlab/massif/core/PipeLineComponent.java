package idlab.massif.core;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.PipeLineElement;

public class PipeLineComponent implements ListenerInf {

	private List<PipeLineComponent> output;
	Map<Integer,List<PipeLineComponent>> outputMap;
	private PipeLineElement element;
	private ExecutorService queue = Executors.newFixedThreadPool(1);
	private boolean isQueryComponentLinked = false;

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
		// add all arriving events to the queue
		queue.execute(new Runnable() {

			@Override
			public void run() {
				element.addEvent(event);

			}

		});
	}

	public void setQueryComponentLinked(boolean queryCompLinked) {
		this.isQueryComponentLinked = queryCompLinked;
	}

	@Override
	public void notify(int queryID, String event) {
		// send all the response from this component to all its output components
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
	
	
	

}
