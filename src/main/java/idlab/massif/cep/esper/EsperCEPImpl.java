package idlab.massif.cep.esper;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.Map.Entry;

import org.semanticweb.owlapi.model.OWLAxiom;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.event.map.MapEventBean;

import idlab.massif.interfaces.core.CEPInf;
import idlab.massif.interfaces.core.CEPListener;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.selection.csparql_basic.StreamingJena;

public class EsperCEPImpl implements CEPInf{

	private EPServiceProvider epService;
	private Set<String> acceptedEvents;

	public EsperCEPImpl() {

	}

	public boolean registerQuery(String query, Set<String> eventTypes, ListenerInf listener) {
		this.acceptedEvents = eventTypes;
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("packedId", "string");
		properties.put("ts", "long");
		properties.put("content", Set.class);
		ClassLoader classLoader = StreamingJena.class.getClassLoader();

		Configuration configuration = new Configuration();
		for (String eventType : eventTypes) {
			configuration.addEventType(eventType, properties);
		}
		// disable internal clock
		configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(true);
		//this.epService = EPServiceProviderManager.getDefaultProvider(configuration);
		this.epService = EPServiceProviderManager.getProvider("cep", configuration);

		// register query
		String eplQuery = "select * from pattern [ " + query + "]";
		EPStatement statement = epService.getEPAdministrator().createEPL(eplQuery);
		EsperListener esperListener = new EsperListener(listener);
		statement.addListener(esperListener);
		return true;
	}

	public void addEvent(String event, String eventNameFull) {
		String eventName=stripFilterName(eventNameFull);
		if (acceptedEvents.contains(eventName)) {
			Map<String, Object> eventMap = new HashMap<String, Object>();
			eventMap.put("ts", System.currentTimeMillis());
			eventMap.put("content", event);

			epService.getEPRuntime().sendEvent(eventMap, eventName);
		} else {
			System.out.println("Unknown type "+eventName);
		}
	}

	private String stripFilterName(String longName) {
		if (longName.contains("#")) {
			return longName.substring(longName.lastIndexOf('#') + 1);

		} else if(longName.contains("/")){
			return longName.substring(longName.lastIndexOf('/') + 1);
		}else {
			return longName;
		}
	}

	private class EsperListener implements UpdateListener {
		private ListenerInf cepListener;

		public EsperListener(ListenerInf cepListener) {
			this.cepListener = cepListener;
		}

		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			try {
				if (newEvents != null) {
					for (EventBean e : newEvents) {
						if (e instanceof MapEventBean) {
							MapEventBean meb = (MapEventBean) e;
							Map<String, Object> cepResult = new HashMap<String, Object>();
							for (Entry<String, Object> entry : meb.getProperties().entrySet()) {
								Map<String, Object> subResult = new HashMap<String, Object>();
								cepResult.put(entry.getKey(), subResult);
								if (entry.getValue() instanceof MapEventBean) {
									MapEventBean mapEvent = (MapEventBean) entry.getValue();
									String eventName = mapEvent.getEventType().getName();
									subResult.put("eventName", eventName);
									if (mapEvent.getProperties().containsKey("content")) {
										String message = (String) mapEvent.getProperties().get("content");
										subResult.put("event", message);
									}
								}
							}
							cepListener.notify(0,cepResult.toString());
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public boolean addEvent(String event) {
		// TODO Auto-generated method stub
		for(String eventType: acceptedEvents) {
			if(event.contains(eventType)) {
				this.addEvent(event, eventType);
			}
		}
		return false;
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
	public int registerQuery(String complexName, String CEPquery, Set<String> eventTypes,
			Map<String, String> indComplexPropMapping) {
		// TODO Auto-generated method stub
		return 0;
	}
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		this.epService.destroy();
	}

	@Override
	public boolean registerQuery(String CEPquery, Set<String> eventTypes, CEPListener listener) {
		// TODO Auto-generated method stub
		return false;
	}
	
	
}
