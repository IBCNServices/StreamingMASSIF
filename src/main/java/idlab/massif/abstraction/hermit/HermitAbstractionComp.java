package idlab.massif.abstraction.hermit;

import java.io.File;
import java.io.FileOutputStream;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.stream.Collectors;

import org.semanticweb.HermiT.Configuration;
import org.semanticweb.HermiT.Reasoner;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.io.StringDocumentTarget;
import org.semanticweb.owlapi.model.IRI;
import org.semanticweb.owlapi.model.OWLAxiom;
import org.semanticweb.owlapi.model.OWLClass;
import org.semanticweb.owlapi.model.OWLClassExpression;
import org.semanticweb.owlapi.model.OWLDataFactory;
import org.semanticweb.owlapi.model.OWLEquivalentClassesAxiom;
import org.semanticweb.owlapi.model.OWLNamedIndividual;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.semanticweb.owlapi.model.OWLOntologyStorageException;
import org.semanticweb.owlapi.model.OWLSubClassOfAxiom;
import org.semanticweb.owlapi.reasoner.NodeSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idlab.massif.abstraction.hermit.utils.DLQueryParser;
import idlab.massif.interfaces.core.AbstractionInf;
import idlab.massif.interfaces.core.AbstractionListenerInf;
import idlab.massif.interfaces.core.ListenerInf;

public class HermitAbstractionComp implements AbstractionInf {
	private OWLOntology ontology;
	private OWLOntologyManager manager;
	private Reasoner reasoner;
	private OWLDataFactory factory;
	private static String EVENT_IRI = "http://idlab.massif.be/EVENT";
	private static String ONTOLOGY_IRI = "http://idlab.massif.be/abstraction.owl#";

	private ListenerInf listener;
	private DLQueryParser parser;
	private int queryCounter = 0;
	private Map<String, Integer> queryIDMapper;
	private Set<String> queries;

	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public HermitAbstractionComp() {
		this.queryIDMapper = new HashMap<String, Integer>();
		this.queries = new HashSet<String>();
	}

	public HermitAbstractionComp(OWLOntology ontology) {
		this();
		setOntology(ontology);
	}

	public HermitAbstractionComp(String ontologyIRI) {
		// load ontology
		this.manager = OWLManager.createOWLOntologyManager();
		try {
			this.ontology = manager.loadOntologyFromOntologyDocument(IRI.create(ontologyIRI));
		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		initiateReasoner();
	}

	private void initiateReasoner() {
		// initiate the reasoner
		Configuration conf = new Configuration();
		conf.ignoreUnsupportedDatatypes = true;
		// conf.prepareReasonerInferences = new PrepareReasonerInferences();
		// conf.prepareReasonerInferences.realisationRequired = true;
		this.reasoner = new Reasoner(conf, this.ontology);
		this.factory = manager.getOWLDataFactory();
	}

	@Deprecated
	public boolean registerDLQuery(String newClass, String classExpression, AbstractionListenerInf listener) {

		return false;
	}

	public void addEvent(Set<OWLAxiom> event) {
		
		System.out.println(event);
		Set<Map<Integer, String>> activatedQueries = new HashSet<Map<Integer, String>>();
		synchronized (ontology) {
			manager.addAxioms(ontology, event);
			try {
			File file = new File("/tmp/newfile.owl");
			FileOutputStream fop = new FileOutputStream(file);
			manager.saveOntology(ontology, fop);
			}catch(Exception e) {
				
			}
			initiateReasoner();

			reasoner.flush();
			OWLClass messageClass = factory.getOWLClass(EVENT_IRI);
			NodeSet<OWLNamedIndividual> eventIndividuals = reasoner.getInstances(messageClass, false);
			logger.debug("Found abstract events: " + eventIndividuals);
			for (OWLNamedIndividual eventInd : eventIndividuals.getFlattened()) {
				if (event.toString().contains(eventInd.toString())) {

					// ask reasoner for the types of the arriving event
					NodeSet<OWLClass> inferedClasses = reasoner.getTypes(eventInd, false);
					for (OWLClass owlclss : inferedClasses.getFlattened()) {
						String clss = owlclss.getIRI().toString();
						if (queries.contains(clss)) {
							// add abstract class to event
							event.add(factory.getOWLClassAssertionAxiom(owlclss, eventInd));
							Map<Integer, String> eventMapping = new HashMap<Integer, String>(1);
							eventMapping.put(queryIDMapper.get(clss), clss);
							activatedQueries.add(eventMapping);

						}
					}

				}
			}
			manager.removeAxioms(ontology, event);

		}
		if (!activatedQueries.isEmpty()) {
			String eventStr = "";
			try {
				OWLOntology eventOnt = manager.createOntology();
				manager.addAxioms(eventOnt, event);
				StringDocumentTarget target = new StringDocumentTarget();
				manager.saveOntology(eventOnt, target);

				eventStr = target.toString();
			} catch (OWLOntologyCreationException | OWLOntologyStorageException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			for (Map<Integer, String> queryMap : activatedQueries) {
				for (Entry<Integer, String> ent : queryMap.entrySet()) {
					listener.notify(ent.getKey(), eventStr);
				}
			}
		}

	}

	@Override
	public boolean setOntology(OWLOntology ontology) {
		// load ontology
		this.manager = ontology.getOWLOntologyManager();
		this.ontology = ontology;
		if(!this.ontology.getOntologyID().getOntologyIRI().isPresent()) {
			try {
				this.ontology = manager.createOntology(IRI.create(ONTOLOGY_IRI));
				manager.addAxioms(this.ontology, ontology.getAxioms());
			} catch (OWLOntologyCreationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}
		// initiate the reasoner
		initiateReasoner();
		this.parser = new DLQueryParser(ontology);
		return true;
	}

	@Override
	public boolean addEvent(String triples) {
		// add import to event
		if (ontology.getOntologyID().getOntologyIRI().isPresent()) {
			triples += "\n[ rdf:type owl:Ontology ;\n" + "   owl:imports <"
					+ ontology.getOntologyID().getOntologyIRI().get() + ">\n" + " ] .";
		}
		logger.debug("Received message: " + triples);
		// load event
		OWLOntology eventOnt;
		try {
			eventOnt = manager.loadOntologyFromOntologyDocument(new StringDocumentSource(triples));
			Set<OWLAxiom> event = eventOnt.axioms().collect(Collectors.toSet());
			this.addEvent(event);
			manager.removeOntology(eventOnt.getOntologyID());
			return true;

		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}

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
	public int registerDLQuery(String newClass, String classExpression) {
		if (!queries.contains(newClass)) {
			OWLClass messageClass = factory.getOWLClass(EVENT_IRI);
			OWLClass newClassAxiom = factory.getOWLClass(newClass);
			OWLSubClassOfAxiom subclassAxiom = factory.getOWLSubClassOfAxiom(newClassAxiom, messageClass);
			OWLClassExpression dlQuery = parser.parseClassExpression(classExpression);
			OWLEquivalentClassesAxiom eqclassAxiom = factory.getOWLEquivalentClassesAxiom(newClassAxiom, dlQuery);

			synchronized (ontology) {
				manager.addAxiom(ontology, subclassAxiom);
				manager.addAxiom(ontology, eqclassAxiom);
				reasoner.flush();
				queryIDMapper.put(newClass, queryCounter);
				queryCounter++;
				queries.add(newClass);
			}
			return queryCounter - 1;
		} else {
			return -1;
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
