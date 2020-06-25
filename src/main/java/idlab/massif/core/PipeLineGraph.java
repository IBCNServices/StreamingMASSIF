package idlab.massif.core;

import java.util.Map;

public class PipeLineGraph {
	
	private Map<String, PipeLineComponent> pipecomps;

	public PipeLineGraph(Map<String,PipeLineComponent> pipecomps) {
		this.pipecomps = pipecomps;
	}
	public void stop() {
		pipecomps.values().stream().forEach(p->p.getElement().stop());
	}

}
