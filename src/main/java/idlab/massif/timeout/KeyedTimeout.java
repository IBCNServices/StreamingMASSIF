package idlab.massif.timeout;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import idlab.massif.filter.jena.JenaFilter;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.PipeLineElement;
import idlab.massif.utils.FormatUtils;
import idlab.massif.utils.TableUtils;

public class KeyedTimeout implements PipeLineElement, ListenerInf {

	private ListenerInf listener;
	private double timeout;
	private double prevMessage;
	private Map<String, Double> keyedTimeout;
	private JenaFilter filter;
	private String query;
	private String currentEvent;

	public KeyedTimeout(String timeout, String query) {
		this.timeout = Double.parseDouble(timeout);
		this.prevMessage = 0;
		this.keyedTimeout = new HashMap<String, Double>();
		this.filter = new JenaFilter();
		this.query = query;
		filter.registerContinuousQuery(query);
		filter.addListener(this);

	}

	@Override
	public boolean addEvent(String event) {
		this.currentEvent = event;
		// extract key
		filter.addEvent(event);

		return true;
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
		return String.format("{\"type\":\"TimeOut\",\"impl\":\"timeout\",\"option1\":\"%s\",\"option2\":\"%s\"}", timeout + "",
				FormatUtils.encodeQuery(query));

	}

	@Override
	public void notify(int queryID, String event) {
		Map<String, List<String>> table = TableUtils.convertStringToTable(event);
		if (table.containsKey("key") && !table.get("key").isEmpty()) {
			String key = table.get("key").get(0);
			double prevMessage = keyedTimeout.getOrDefault(key, (double) 0);
			if (System.currentTimeMillis() - prevMessage > timeout) {
				listener.notify(0, this.currentEvent);
				keyedTimeout.put(key,(double)System.currentTimeMillis());
			}
		}

	}

}
