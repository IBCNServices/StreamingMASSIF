package idlab.massif.run;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import org.json.JSONArray;
import org.json.JSONObject;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import idlab.massif.abstraction.hermit.HermitAbstractionImpl;
import idlab.massif.core.PipeLineComponent;
import idlab.massif.core.PipeLineGraph;
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

	public static PipeLineComponent parseComponent(JSONObject comp) {
		String compType = comp.getString("type").toLowerCase();
		PipeLineComponent pipeComp = null;
		switch (compType) {
		case "sink":
			String impl = comp.getString("impl").toLowerCase();
			if (impl.equals("printsink")) {
				SinkInf printSink = new PrintSink();
				pipeComp = new PipeLineComponent(printSink, Collections.EMPTY_LIST);
			} else if (impl.equals("websocketsink")) {

				SinkInf socketSink = new WebSocketServerSink(comp.getInt("port"), comp.getString("path"));
				pipeComp = new PipeLineComponent(socketSink, Collections.EMPTY_LIST);
			} else if (impl.equals("httpgetsink")) {

				SinkInf getsink = new HTTPGetSink(comp.getString("path"), comp.getString("config"));
				pipeComp = new PipeLineComponent(getsink, Collections.EMPTY_LIST);
			} else if (impl.equals("httpgetsinkcombined")) {

				SinkInf getsink = new HTTPGetCombinedSink(comp.getString("path"), comp.getString("config"));
				pipeComp = new PipeLineComponent(getsink, Collections.EMPTY_LIST);
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
					int filterQueryID = filter.registerContinuousQuery(queries.getString(i));
				}
				filter.start();
				pipeComp = new PipeLineComponent(filter, Collections.EMPTY_LIST);
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
			pipeComp = new PipeLineComponent(window, Collections.EMPTY_LIST);
			break;
		case "abstract":
			// code block
			AbstractionInf abstractor = new HermitAbstractionImpl();
			if (comp.has("ontologyIRI")) {
				String ontologyIRI = comp.getString("ontologyIRI");
				OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
				OWLOntology ontology;
				try {
					ontology = manager.loadOntology(IRI.create(ontologyIRI));
					abstractor.setOntology(ontology);
				} catch (OWLOntologyCreationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}

			}
			JSONArray queries = comp.getJSONArray("expressions");
			for (int i = 0; i < queries.length(); i++) {
				JSONObject exp = queries.getJSONObject(i);
				String head = exp.getString("head");
				String tail = exp.getString("tail");
				abstractor.registerDLQuery(head, tail);
			}
			pipeComp = new PipeLineComponent(abstractor, Collections.EMPTY_LIST);
			break;
		case "source":
			impl = comp.getString("impl").toLowerCase();
			if (impl.equals("kafkasource")) {
				KafkaSource kafkaSource = new KafkaSource(comp.getString("kafkaServer"), comp.getString("kafkaTopic"));
				pipeComp = new PipeLineComponent(kafkaSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("httppostsource")) {
				HTTPPostSource postSource = new HTTPPostSource(comp.getString("path"), comp.getInt("port"));
				pipeComp = new PipeLineComponent(postSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("httpgetsource")) {
				HTTPGetSource getSource = new HTTPGetSource(comp.getString("url"), comp.getInt("timeout"));
				pipeComp = new PipeLineComponent(getSource, Collections.EMPTY_LIST);
			}
			if (impl.equals("filesource")) {
				SourceInf fileSource = new FileSource(comp.getString("fileName"), comp.getInt("timeout"));
				pipeComp = new PipeLineComponent(fileSource, Collections.EMPTY_LIST);
			}
			break;
		case "mapper":
			String mapping = comp.getString("mapping");
			MapperInf mapper = new SimpleMapper(mapping);
			pipeComp = new PipeLineComponent(mapper, Collections.EMPTY_LIST);

		}
		return pipeComp;
	}

	public static PipeLineGraph parse(String query) throws OWLOntologyCreationException {
		Map<String, PipeLineComponent> pipelineComponents = new HashMap<String, PipeLineComponent>();
		JSONObject obj = new JSONObject(query);
		if (!obj.has("components")) {
			return null;
		} else {
			JSONObject components = obj.getJSONObject("components");

			for (String key : components.keySet()) {
				JSONObject comp = components.getJSONObject(key);

				pipelineComponents.put(key, parseComponent(comp));
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
