package idlab.massif.run;

import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.json.JSONArray;
import org.json.JSONObject;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idlab.massif.abstraction.hermit.HermitAbstractionImpl;
import idlab.massif.cep.esper.EsperCEPImpl;
import idlab.massif.core.PipeLineComponent;
import idlab.massif.core.PipeLineGraph;
import idlab.massif.exceptions.QueryRegistrationException;
import idlab.massif.filter.jena.JenaFilter;
import idlab.massif.interfaces.core.AbstractionInf;
import idlab.massif.interfaces.core.FilterInf;
import idlab.massif.interfaces.core.MapperInf;
import idlab.massif.interfaces.core.SinkInf;
import idlab.massif.interfaces.core.SourceInf;
import idlab.massif.interfaces.core.WindowInf;
import idlab.massif.mapping.SimpleMapper;
import idlab.massif.sinks.HTTPGetCombinedSink;
import idlab.massif.sinks.HTTPGetSink;
import idlab.massif.sinks.PrintSink;
import idlab.massif.sinks.WebSocketServerSink;
import idlab.massif.sources.FileSource;
import idlab.massif.sources.HTTPGetSource;
import idlab.massif.sources.HTTPPostSource;
import idlab.massif.sources.KafkaSource;
import idlab.massif.window.esper.EsperWindow;

public class QueryParser {
	private static Logger logger = LoggerFactory.getLogger(QueryParser.class);

	public static PipeLineComponent parseComponent(String compId, JSONObject comp) throws QueryRegistrationException{
		String compType = comp.getString("type").toLowerCase();
		PipeLineComponent pipeComp = null;
		switch (compType) {
		case "sink":
			String impl = comp.getString("impl").toLowerCase();
			if (impl.equals("printsink")) {
				SinkInf printSink = new PrintSink();
				pipeComp = new PipeLineComponent(compId, printSink, Collections.EMPTY_LIST);
			} else if (impl.equals("websocketsink")) {

				SinkInf socketSink = new WebSocketServerSink(comp.getInt("port"), comp.getString("path"));
				pipeComp = new PipeLineComponent(compId, socketSink, Collections.EMPTY_LIST);
			} else if (impl.equals("httpgetsink")) {

				SinkInf getsink = new HTTPGetSink(comp.getString("path"), comp.getString("config"));
				pipeComp = new PipeLineComponent(compId, getsink, Collections.EMPTY_LIST);
			} else if (impl.equals("httpgetsinkcombined")) {

				SinkInf getsink = new HTTPGetCombinedSink(comp.getString("path"), comp.getString("config"));
				pipeComp = new PipeLineComponent(compId, getsink, Collections.EMPTY_LIST);
			}
			// code block
			break;
		case "filter":
			impl = "jena";
			if (comp.has("impl")) {
				impl = comp.getString("impl");
			}
			if (impl.equals("jena")) {

				FilterInf filter = new JenaFilter();
				if (comp.has("ontology")) {
					filter.setStaticData(comp.getString("ontology"));
				}
				JSONArray queries = comp.getJSONArray("queries");
				for (int i = 0; i < queries.length(); i++) {
					try {
						int filterQueryID = filter.registerContinuousQuery(queries.getString(i));
					} catch (Exception e) {
						throw new QueryRegistrationException(String.format("Unable to register filter:\n [%s].\n Parsing error: [%s]",queries.getString(i),e.getMessage()), e);
					}
				}
				filter.start();
				pipeComp = new PipeLineComponent(compId, filter, Collections.EMPTY_LIST);
			}
			// code block
			break;
		case "window":
			int size = comp.getInt("size");
			int slide = size;
			if (comp.has("slide")) {
				slide = comp.getInt("slide");
			}
			WindowInf window = new EsperWindow();
			window.setWindowSize(size, slide);
			window.start();
			pipeComp = new PipeLineComponent(compId, window, Collections.EMPTY_LIST);
			break;
		case "abstract":
			// code block
			AbstractionInf abstractor = new HermitAbstractionImpl();
			if (comp.has("ontologyIRI")) {
				String ontologyIRI = comp.getString("ontologyIRI");
				OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
				OWLOntology ontology;
				try {
					if (ontologyIRI.startsWith("http")) {
						ontology = manager.loadOntology(IRI.create(ontologyIRI));
					} else {
						ontology = manager.loadOntologyFromOntologyDocument(new File(ontologyIRI));
					}
					abstractor.setOntology(ontology);
				} catch (OWLOntologyCreationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			pipeComp = new PipeLineComponent(compId, abstractor, Collections.EMPTY_LIST);
			JSONArray queries = comp.getJSONArray("expressions");
			for (int i = 0; i < queries.length(); i++) {
				JSONObject exp = queries.getJSONObject(i);
				String head = exp.getString("head");
				String tail = exp.getString("tail");
				abstractor.registerDLQuery(head, tail);
			}

			break;
		case "source":
			impl = comp.getString("impl").toLowerCase();
			if (impl.equals("kafkasource")) {
				KafkaSource kafkaSource = new KafkaSource(comp.getString("kafkaServer"), comp.getString("kafkaTopic"));
				pipeComp = new PipeLineComponent(compId, kafkaSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("httppostsource")) {
				HTTPPostSource postSource = new HTTPPostSource(comp.getString("path"), comp.getInt("port"));
				pipeComp = new PipeLineComponent(compId, postSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("httpgetsource")) {
				HTTPGetSource getSource = new HTTPGetSource(comp.getString("url"), comp.getInt("timeout"));
				pipeComp = new PipeLineComponent(compId, getSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("filesource")) {
				SourceInf fileSource = new FileSource(comp.getString("fileName"), comp.getInt("timeout"));
				pipeComp = new PipeLineComponent(compId, fileSource, Collections.EMPTY_LIST);
			}
			break;
		case "mapper":
			String mapping = comp.getString("mapping");
			boolean keepHeader = false;
			if (comp.has("keepHeader")) {
				keepHeader = comp.getBoolean("keepHeader");
			}
			MapperInf mapper = new SimpleMapper(mapping, keepHeader);
			pipeComp = new PipeLineComponent(compId, mapper, Collections.EMPTY_LIST);
			break;
		case "cep":
			String query = comp.getString("query");
			String classes = comp.getString("classes");
			EsperCEPImpl cepEngine = new EsperCEPImpl();
			Set<String> eventTypes = new HashSet<String>();
			classes = classes.replace(" ", "");
			for (String claz : classes.split(",")) {
				eventTypes.add(claz);
			}
			pipeComp = new PipeLineComponent(compId, cepEngine, Collections.EMPTY_LIST);
			cepEngine.registerQuery(query, eventTypes, pipeComp);

		}
		return pipeComp;
	}

	public static PipeLineGraph parse(String query) throws QueryRegistrationException {
		Map<String, PipeLineComponent> pipelineComponents = new HashMap<String, PipeLineComponent>();
		JSONObject obj = new JSONObject(query);
		if (!obj.has("components")) {
			return null;
		} else {
			JSONObject components = obj.getJSONObject("components");

			for (String key : components.keySet()) {
				JSONObject comp = components.getJSONObject(key);
				pipelineComponents.put(key, parseComponent(key, comp));
				
			}
		}
		// configure the graph
		JSONObject configs = obj.getJSONObject("configuration");
		for (String key : configs.keySet()) {
			JSONArray linked = configs.getJSONArray(key);
			ArrayList<PipeLineComponent> linkedComps = new ArrayList<PipeLineComponent>();
			for (int i = 0; i < linked.length(); i++) {
				linkedComps.add(pipelineComponents.get(linked.get(i).toString()));
			}
			pipelineComponents.get(key).setOutput(linkedComps);
		}
		// start the sources

		for (PipeLineComponent comp : pipelineComponents.values()) {
			if (comp.getElement() instanceof SourceInf) {
				comp.getElement().start();
			}
		}

		return new PipeLineGraph(pipelineComponents);
	}

}
