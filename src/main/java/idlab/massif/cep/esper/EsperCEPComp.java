package idlab.massif.cep.esper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.rdf.model.ResourceFactory;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.vocabulary.RDF;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

public class EsperCEPComp implements CEPInf {

	private EPServiceProvider epService;

	private Set<String> allEventTypes;
	private List<String> queries;
	private List<String> complexNames;
	private List<Map<String,String>> indComplexPropMappings;
	private int queryCounter;

	private ListenerInf listener;

	private Query typeQuery;

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public EsperCEPComp() {
		this.allEventTypes = new HashSet<String>();
		this.queryCounter = 0;
		this.typeQuery=	QueryFactory.create("Select DISTINCT ?ind ?someType WHERE {?ind <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> ?someType}" );
		this.queries = new ArrayList<String>();
		this.complexNames = new ArrayList<String>();
		indComplexPropMappings = new ArrayList<Map<String,String>>();
	}

	public int registerQuery(String complexName,String query, Set<String> eventTypes,Map<String,String> indComplexPropMapping) {
		allEventTypes.addAll(eventTypes);
		queries.add(query);
		complexNames.add(complexName);
		indComplexPropMappings.add(indComplexPropMapping);
		queryCounter++;
		return queryCounter - 1;

	}

	@Deprecated
	public boolean registerQuery(String query, Set<String> eventTypes, CEPListener listener) {
		return false;
	}
	@Deprecated
	public void addEvent(String event, String eventNameFull) {
		
	}

	private String stripFilterName(String longName) {
		if (longName.contains("#")) {
			return longName.substring(longName.lastIndexOf('#') + 1);

		} else {
			return longName.substring(longName.lastIndexOf('/') + 1);
		}
	}

	@Override
	public boolean addEvent(String event) {
		logger.debug("Received message" + event);
		List<String> foundTypes = new ArrayList<String>();
		List<String> foundInds = new ArrayList<String>();

		Model dataModel = ModelFactory.createDefaultModel();
		try {
			InputStream targetStream = new ByteArrayInputStream(event.getBytes());
			dataModel.read(targetStream,"TTL");
			StmtIterator it = dataModel.listStatements();
			try (QueryExecution qexec = QueryExecutionFactory.create(typeQuery, dataModel)) {
				ResultSet result = qexec.execSelect();
				while(result.hasNext()){
					
					QuerySolution sol = result.nextSolution();
					String stripped =stripFilterName(sol.get("someType").toString());
					if(allEventTypes.contains(stripped)) {
						foundTypes.add(stripped);
						foundInds.add(sol.get("ind").toString());
					}
				}
			}catch(Exception e) {
				e.printStackTrace();
			}
		}catch(Exception e) {
			e.printStackTrace();
		}
		logger.debug("Found types: " + foundTypes);
		for(int i = 0 ; i <foundTypes.size();i++) {			
				Map<String, Object> eventMap = new HashMap<String, Object>();
				eventMap.put("ts", System.currentTimeMillis());
				eventMap.put("content", dataModel);	
				eventMap.put("ind", foundInds.get(i));	
				epService.getEPRuntime().sendEvent(eventMap, foundTypes.get(i));	
		}
		return true;
	}
	

	@Override
	public boolean addListener(ListenerInf listener) {
		this.listener = listener;
		return true;
	}

	@Override
	public void start() {
		Map<String, Object> properties = new HashMap<String, Object>();
		properties.put("packedId", "string");
		properties.put("ts", "long");
		properties.put("content", Model.class);
		properties.put("ind", "string");

		ClassLoader classLoader = StreamingJena.class.getClassLoader();

		Configuration configuration = new Configuration();
		for (String eventType : allEventTypes) {
			configuration.addEventType(eventType, properties);
		}
		// disable internal clock
		configuration.getEngineDefaults().getThreading().setInternalTimerEnabled(true);
		// this.epService = EPServiceProviderManager.getDefaultProvider(configuration);
		this.epService = EPServiceProviderManager.getProvider("cep", configuration);
		for(int i = 0 ; i < queries.size();i++) {
			// register query
			String query = queries.get(i);
			String eplQuery = "select * from pattern [ " + query + "]";
			EPStatement statement = epService.getEPAdministrator().createEPL(eplQuery);
			EsperListener esperListener = new EsperListener(i,complexNames.get(i),indComplexPropMappings.get(i));
			statement.addListener(esperListener);
		}
	}

	private class EsperListener implements UpdateListener {
		private int queryID;
		private String complexName;
		private Map<String,String> indMapping;
		public EsperListener(int queryID,String complexName,Map<String,String> indMapping) {
			this.queryID = queryID;
			this.complexName=complexName;
			this.indMapping =indMapping;
			
		}

		public void update(EventBean[] newEvents, EventBean[] oldEvents) {
			try {
				if (newEvents != null) {
					for (EventBean e : newEvents) {
						if (e instanceof MapEventBean) {
							MapEventBean meb = (MapEventBean) e;
							Model cepResult = ModelFactory.createDefaultModel();
							//add the complex class to the model
							Resource blank = cepResult.createResource();
							cepResult.add(ResourceFactory.createStatement(blank, RDF.type, ResourceFactory.createResource(complexName)));
							for (Entry<String, Object> entry : meb.getProperties().entrySet()) {
								
								if (entry.getValue() instanceof MapEventBean) {
									MapEventBean mapEvent = (MapEventBean) entry.getValue();
									String eventName = mapEvent.getEventType().getName();
									String eventIdentifier = entry.getKey();
									if (mapEvent.getProperties().containsKey("content")) {
										cepResult.add(ResourceFactory.createStatement(blank, 
												ResourceFactory.createProperty(indMapping.get(eventIdentifier)), 
												ResourceFactory.createResource((String)mapEvent.getProperties().get("ind"))));

										Model eventModel = (Model) mapEvent.getProperties().get("content");
										cepResult.add(eventModel);
									}
								}
							}
							String syntax = "RDF/XML"; // also try "N-TRIPLE" and "TURTLE"
							StringWriter out = new StringWriter();
							cepResult.write(out, syntax);
							String resultString = out.toString();
							listener.notify(this.queryID,resultString);
						}
					}
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		this.epService.destroy();
	}

}
