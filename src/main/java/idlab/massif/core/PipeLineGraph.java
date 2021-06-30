package idlab.massif.core;

import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class PipeLineGraph {
	
	private Map<String, PipeLineComponent> pipecomps;

	public PipeLineGraph(Map<String,PipeLineComponent> pipecomps) {
		this.pipecomps = pipecomps;
	}
	public void stop() {
		pipecomps.values().stream().forEach(p->p.getElement().stop());
	}
	@Override
	public String toString() {
		Map<PipeLineComponent, String> reverseMap = new HashMap<PipeLineComponent, String>();
		StringBuilder str = new StringBuilder();
		//Add components definitions
		str.append("{\"components\":{");
		for (Entry<String,PipeLineComponent> entry:pipecomps.entrySet()) {
			str.append("\"").append(entry.getKey()).append("\":").append(entry.getValue().getConfig()).append(",");
			reverseMap.put(entry.getValue(), entry.getKey());
		}
		//remove trailing comma fix
		str.deleteCharAt(str.length()-1);
		str.append("},");
		//Add configuration
		str.append("\"configuration\":{");
		for (Entry<String,PipeLineComponent> entry:pipecomps.entrySet()) {
			reverseMap.put(entry.getValue(), entry.getKey());
			str.append("\"").append(entry.getKey()).append("\":[");
			if(!entry.getValue().getOutputs().isEmpty()) {
				for (PipeLineComponent out : entry.getValue().getOutputs()) {
					str.append("\"").append(reverseMap.get(out)).append("\",");
				}
				//remove trailing comma fix
				str.deleteCharAt(str.length() - 1);
			}
			str.append("],");
		}
		//remove trailing comma fix
		str.deleteCharAt(str.length()-1);
		str.append("}}");
		return str.toString();
	}

}
