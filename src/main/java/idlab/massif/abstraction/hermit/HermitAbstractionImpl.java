package idlab.massif.abstraction.hermit;

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

import idlab.massif.abstraction.hermit.utils.DLQueryParser;
import idlab.massif.interfaces.core.AbstractionInf;
import idlab.massif.interfaces.core.AbstractionListenerInf;
import idlab.massif.interfaces.core.ListenerInf;

public class HermitAbstractionImpl implements AbstractionInf {
	private OWLOntology ontology;
	private OWLOntologyManager manager;
	private Reasoner reasoner;
	private OWLDataFactory factory;
	private static String EVENT_IRI = "http://idlab.massif.be/EVENT";
	private Map<String, AbstractionListenerInf> listenerMapping;
	private DLQueryParser parser;

	public HermitAbstractionImpl() {
		this.manager = OWLManager.createOWLOntologyManager();
		try {
			this.ontology = manager.createOntology();
			initiateReasoner();
		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public HermitAbstractionImpl(OWLOntology ontology) {
		// load ontology
		this.manager = ontology.getOWLOntologyManager();
		this.ontology = ontology;
		// initiate the reasoner
		initiateReasoner();
		this.parser = new DLQueryParser(ontology);
		this.listenerMapping = new HashMap<String, AbstractionListenerInf>();

	}

	public HermitAbstractionImpl(String ontologyIRI) {
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

	public boolean registerDLQuery(String newClass, String classExpression, AbstractionListenerInf listener) {
		OWLClass messageClass = factory.getOWLClass(EVENT_IRI);
		OWLClass newClassAxiom = factory.getOWLClass(newClass);
		OWLSubClassOfAxiom subclassAxiom = factory.getOWLSubClassOfAxiom(newClassAxiom, messageClass);
		OWLClassExpression dlQuery = parser.parseClassExpression(classExpression);
		OWLEquivalentClassesAxiom eqclassAxiom = factory.getOWLEquivalentClassesAxiom(newClassAxiom, dlQuery);

		synchronized (ontology) {
			manager.addAxiom(ontology, subclassAxiom);
			manager.addAxiom(ontology, eqclassAxiom);
			reasoner.flush();
			// TODO check if this is correct!!
			listenerMapping.put(newClass, listener);
		}
		return true;
	}

	public void addEvent(Set<OWLAxiom> event) {
		Set<Map<AbstractionListenerInf, String>> activatedLsitener = new HashSet<Map<AbstractionListenerInf, String>>();
		synchronized (ontology) {
			manager.addAxioms(ontology, event);
			// TODO: fix reinitation!!!
			// initiateReasoner();

			reasoner.flush();
			OWLClass messageClass = factory.getOWLClass(EVENT_IRI);
			NodeSet<OWLNamedIndividual> eventIndividuals = reasoner.getInstances(messageClass, false);
			for (OWLNamedIndividual eventInd : eventIndividuals.getFlattened()) {
				if (event.toString().contains(eventInd.toString())) {

					// ask reasoner for the types of the arriving event
					NodeSet<OWLClass> inferedClasses = reasoner.getTypes(eventInd, false);
					for (OWLClass owlclss : inferedClasses.getFlattened()) {
						String clss = owlclss.getIRI().toString();
						if (listenerMapping.containsKey(clss)) {
							// add abstract class to event
							event.add(factory.getOWLClassAssertionAxiom(owlclss, eventInd));
							Map<AbstractionListenerInf, String> eventMapping = new HashMap<AbstractionListenerInf, String>(
									1);
							eventMapping.put(listenerMapping.get(clss), clss);
							activatedLsitener.add(eventMapping);

						}
					}

				}
			}
		}
		if (!activatedLsitener.isEmpty()) {
			String eventStr="";
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
			
			for (Map<AbstractionListenerInf, String> listenerMap : activatedLsitener) {
				for (Entry<AbstractionListenerInf, String> ent : listenerMap.entrySet()) {
					ent.getKey().notify(eventStr, ent.getValue());
				}
			}
		}

	}

	@Override
	public boolean setOntology(OWLOntology ontology) {
		// load ontology
		this.manager = ontology.getOWLOntologyManager();
		this.ontology = ontology;
		// initiate the reasoner
		initiateReasoner();
		this.parser = new DLQueryParser(ontology);
		this.listenerMapping = new HashMap<String, AbstractionListenerInf>();
		return true;
	}

	@Override
	public boolean addEvent(String triples) {
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
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public int registerDLQuery(String newClass, String classExpression) {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		
	}




}
