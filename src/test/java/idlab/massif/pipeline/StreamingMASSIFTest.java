package idlab.massif.pipeline;

import java.io.File;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.junit.Test;
import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;

import idlab.massif.abstraction.hermit.HermitAbstractionComp;
import idlab.massif.abstraction.hermit.HermitAbstractionImpl;
import idlab.massif.cep.esper.EsperCEPComp;
import idlab.massif.core.PipeLineComponent;
import idlab.massif.filter.jena.JenaFilter;
import idlab.massif.interfaces.core.AbstractionInf;
import idlab.massif.interfaces.core.CEPInf;
import idlab.massif.interfaces.core.FilterInf;
import idlab.massif.interfaces.core.SinkInf;
import idlab.massif.interfaces.core.WindowInf;
import idlab.massif.sinks.PrintSink;
import idlab.massif.sources.FileSource;
import idlab.massif.window.esper.EsperWindow;

public class StreamingMASSIFTest {

	@Test
	public void test() throws InterruptedException, OWLOntologyCreationException {
		//define window
		WindowInf window = new EsperWindow();
		window.setWindowSize(1);
		window.start();
		
		//define filter
		String query = "PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>\n" + 
				"PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + 
				"PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>\n" + 
				"PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>\n" + 
				"PREFIX ssn: <http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#>\n" + 
				"PREFIX dul: <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#>\n" + 
				"CONSTRUCT {\n" + 
					"	?sensor rdf:type <http://IBCNServices.github.io/Accio-Ontology/SSNiot#TemperatureSensor>.\n" + 
					"	?sensor <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation> ?loc.\n" + 
					"	?sensor <http://IBCNServices.github.io/homelab.owl#hasIntensity> ?intensity.\n" + 
					"	?patient <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation> ?loc.\n" + 
					"	?patient <http://IBCNServices.github.io/homelab.owl#hasDisease> ?disease.\n" + 
					"	?disease <http://IBCNServices.github.io/homelab.owl#hasStimulusSensitivity> ?sensitivity.\n" + 
					"	?sensitivity <http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty> ?intensity.\n" + 
					"\n" + 
					"	?observation ?obP ?obO." +
					 "?observation ssn:observedBy ?sensor.\n" + 
					"	?observation ssn:hasValue ?result.\n" + 
					"	?result ?rP ?rO.\n" + 
					"}\n" +
				"WHERE {\n" + 
				"	?sensor rdf:type <http://IBCNServices.github.io/Accio-Ontology/SSNiot#TemperatureSensor>.\n" + 
				"	?sensor <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation> ?loc.\n" + 
				"	?sensor <http://IBCNServices.github.io/homelab.owl#hasIntensity> ?intensity.\n" + 
				"	?patient <http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation> ?loc.\n" + 
				"	?patient <http://IBCNServices.github.io/homelab.owl#hasDisease> ?disease.\n" + 
				"	?disease <http://IBCNServices.github.io/homelab.owl#hasStimulusSensitivity> ?sensitivity.\n" + 
				"	?sensitivity <http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty> ?intensity.\n" + 
				"\n" + 
				"	?observation ssn:observedBy ?sensor.\n" +
		
				"	?observation ?obP ?obO." +

				"	?observation ssn:hasValue ?result.\n" + 
				"	?result ?rP ?rO.\n" + 
				"}";
		FilterInf filter = new JenaFilter();
		filter.setStaticData("examples/influenza/static.owl");
		int filterQueryID=filter.registerContinuousQuery(query);
		filter.start();
		//define the abstraction layer
		AbstractionInf abstractor = new HermitAbstractionComp();

		OWLOntologyManager manager = OWLManager.createOWLOntologyManager();
		OWLOntology ontology = manager.loadOntologyFromOntologyDocument(new StringDocumentSource(ONT_STRING));
		
		ontology = manager.loadOntologyFromOntologyDocument(new File("examples/influenza/influenza.owl"));
		abstractor.setOntology(ontology);
		// register new DL query
		String classExpressiona = "LowTemperatureObservation";// and (observedProperty some Temperature)";

		String classExpressionb = "HighTemperatureObservation ";//and (observedProperty some Temperature)";

		String newHead = "http://massif.test/LowTemperatureEvent";
		String newHead2 = "http://massif.test/HighTemperatureEvent";

		int abstractionQueryID=abstractor.registerDLQuery(newHead, classExpressiona);
		int abstractionQueryID2=abstractor.registerDLQuery(newHead2, classExpressionb);

		//define CEP component
		String querySub = "a=LowTemperatureEvent -> b=HighTemperatureEvent where timer:within(60 sec) ";
		//querySub="a=GraphEvent";
		Set<String> eventTypes = new HashSet<String>();
		eventTypes.add("LowTemperatureEvent");
		eventTypes.add("HighTemperatureEvent");
		CEPInf cep = new EsperCEPComp();
		Map<String,String> indMappings = new HashMap<String,String>();
		indMappings.put("a","http://massif.test/hasComp");
		indMappings.put("b","http://massif.test/hasComp");
		int cepQueryID=cep.registerQuery("http://massif.test/ComplexEvent", querySub, eventTypes, indMappings);
		cep.start();
		
		//define sink component
		SinkInf printSink = new PrintSink();
		
		
		PipeLineComponent sinkComp = new PipeLineComponent(printSink,Collections.EMPTY_LIST);
		
		PipeLineComponent cepComp = new PipeLineComponent(cep,Collections.singletonList(sinkComp));

		PipeLineComponent abstractionComp = new PipeLineComponent(abstractor,Collections.singletonList(cepComp));

		PipeLineComponent filterComp = new PipeLineComponent(filter,Collections.singletonList(abstractionComp));

		PipeLineComponent windowComp = new PipeLineComponent(window,Collections.singletonList(filterComp));
		
//		for(int i = 0;i<10;i++) {
//			windowComp.addEvent(ONT_EVENTa);
//			Thread.sleep(1000);
//			windowComp.addEvent(ONT_EVENTb);
//
//		}
		FileSource fsource = new FileSource("examples/influenza/stream.xml",1000);
		PipeLineComponent sourceComp = new PipeLineComponent(fsource,Collections.singletonList(filterComp));
		fsource.stream();
		
		Thread.sleep(50000);
	}
	private static String ONT_EVENTa="<?xml version=\"1.0\"?>\n" + 
			"<rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\"\n" + 
			"     xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\"\n" + 
			"     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n" + 
			"     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" + 
			"     xmlns:ssn=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#\"\n" + 
			"     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n" + 
			"     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n" + 
			"     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n" + 
			"    <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus2.owl\">\n" + 
			"        <owl:imports rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl\"/>\n" + 
			"    </owl:Ontology>\n" + 
			"    \n" +  
			"\n" + 
			"\n" + 
			"    <!-- \n" + 
			"    ///////////////////////////////////////////////////////////////////////////////////////\n" + 
			"    //\n" + 
			"    // Individuals\n" + 
			"    //\n" + 
			"    ///////////////////////////////////////////////////////////////////////////////////////\n" + 
			"     -->\n" + 
			"\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#lightIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#motionIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#motionIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelabPlus.owl#obs -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#obsa\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n" + 
			"        <ssn:observedProperty rdf:resource=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"</rdf:RDF>";
	private static String ONT_EVENTb="<?xml version=\"1.0\"?>\n" + 
			"<rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\"\n" + 
			"     xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\"\n" + 
			"     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n" + 
			"     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n" + 
			"     xmlns:ssn=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#\"\n" + 
			"     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n" + 
			"     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n" + 
			"     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n" + 
			"    <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus2.owl\">\n" + 
			"        <owl:imports rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl\"/>\n" + 
			"    </owl:Ontology>\n" + 
			"    \n" +  
			"\n" + 
			"\n" + 
			"    <!-- \n" + 
			"    ///////////////////////////////////////////////////////////////////////////////////////\n" + 
			"    //\n" + 
			"    // Individuals\n" + 
			"    //\n" + 
			"    ///////////////////////////////////////////////////////////////////////////////////////\n" + 
			"     -->\n" + 
			"\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#lightIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#motionIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#motionIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"    \n" + 
			"\n" + 
			"\n" + 
			"    <!-- http://IBCNServices.github.io/homelabPlus.owl#obs -->\n" + 
			"\n" + 
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#obsb\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n" + 
			"        <ssn:observedProperty rdf:resource=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"</rdf:RDF>";
	private static String ONT_STRING = "<?xml version=\"1.0\"?>\n"
			+ "<rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\"\n"
			+ "     xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\"\n"
			+ "     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
			+ "     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
			+ "     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n"
			+ "     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n"
			+ "     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n"
			+ "    <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl\"/>\n" + "    \n" + "\n"
			+ "\n" + "    <!-- \n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "    //\n" + "    // Object Properties\n" + "    //\n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "     -->\n" + "\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation\">\n"
			+ "        <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/>\n"
			+ "    </owl:ObjectProperty>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>\n"
			+ "    \n" + "\n" + "\n" + "    <!-- http://IBCNServices.github.io/homelab.owl#belongsTo -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#belongsTo\">\n"
			+ "        <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/>\n"
			+ "    </owl:ObjectProperty>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#isLinkedTo -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/>\n"
			+ "    \n" + "\n" + "\n" + "    <!-- http://IBCNServices.github.io/homelab.owl#updates -->\n" + "\n"
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#updates\"/>\n" + "    \n"
			+ "\n" + "\n" + "    <!-- \n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "    //\n" + "    // Data properties\n" + "    //\n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "     -->\n" + "\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue -->\n" + "\n"
			+ "    <owl:DatatypeProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>\n"
			+ "    \n" + "\n" + "\n" + "    <!-- \n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "    //\n" + "    // Classes\n" + "    //\n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "     -->\n" + "\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>\n"
			+ "    \n" + "\n" + "\n" + "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion -->\n"
			+ "\n" + "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>\n"
			+ "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected\">\n"
			+ "        <owl:equivalentClass rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\"/>\n"
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    </owl:Class>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n" + "    \n"
			+ "\n" + "\n" + "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation -->\n"
			+ "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    \n" + "\n" + "\n" + "    <!-- http://IBCNServices.github.io/homelab.owl#ActuatorUpdate -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#ActuatorUpdate\"/>\n" + "    \n"
			+ "\n" + "\n" + "    <!-- http://IBCNServices.github.io/homelab.owl#Patient -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#Patient\"/>\n" + "    \n" + "\n"
			+ "\n" + "    <!-- http://IBCNServices.github.io/homelab.owl#StaffMember -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#StaffMember\"/>\n" + "    \n" + "\n"
			+ "\n" + "    <!-- http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation\">\n"
			+ "        <owl:equivalentClass>\n" + "            <owl:Class>\n"
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\">\n"
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>\n"
			+ "                        <owl:someValuesFrom>\n" + "                            <owl:Restriction>\n"
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>\n"
			+ "                                <owl:someValuesFrom>\n"
			+ "                                    <owl:Restriction>\n"
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>\n"
			+ "                                        <owl:someValuesFrom>\n"
			+ "                                            <rdfs:Datatype>\n"
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>\n"
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\">\n"
			+ "                                                    <rdf:Description>\n"
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">30.0</xsd:minExclusive>\n"
			+ "                                                    </rdf:Description>\n"
			+ "                                                </owl:withRestrictions>\n"
			+ "                                            </rdfs:Datatype>\n"
			+ "                                        </owl:someValuesFrom>\n"
			+ "                                    </owl:Restriction>\n"
			+ "                                </owl:someValuesFrom>\n"
			+ "                            </owl:Restriction>\n" + "                        </owl:someValuesFrom>\n"
			+ "                    </owl:Restriction>\n" + "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>\n"
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>\n"
			+ "                    </owl:Restriction>\n" + "                </owl:intersectionOf>\n"
			+ "            </owl:Class>\n" + "        </owl:equivalentClass>\n"
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    </owl:Class>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation\">\n"
			+ "        <owl:equivalentClass>\n" + "            <owl:Class>\n"
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\">\n"
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>\n"
			+ "                        <owl:someValuesFrom>\n" + "                            <owl:Restriction>\n"
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>\n"
			+ "                                <owl:someValuesFrom>\n"
			+ "                                    <owl:Restriction>\n"
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>\n"
			+ "                                        <owl:someValuesFrom>\n"
			+ "                                            <rdfs:Datatype>\n"
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>\n"
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\">\n"
			+ "                                                    <rdf:Description>\n"
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">0.0</xsd:minExclusive>\n"
			+ "                                                    </rdf:Description>\n"
			+ "                                                </owl:withRestrictions>\n"
			+ "                                            </rdfs:Datatype>\n"
			+ "                                        </owl:someValuesFrom>\n"
			+ "                                    </owl:Restriction>\n"
			+ "                                </owl:someValuesFrom>\n"
			+ "                            </owl:Restriction>\n" + "                        </owl:someValuesFrom>\n"
			+ "                    </owl:Restriction>\n" + "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>\n"
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>\n"
			+ "                    </owl:Restriction>\n" + "                </owl:intersectionOf>\n"
			+ "            </owl:Class>\n" + "        </owl:equivalentClass>\n"
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    </owl:Class>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation\">\n"
			+ "        <owl:equivalentClass>\n" + "            <owl:Class>\n"
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\">\n"
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>\n"
			+ "                        <owl:someValuesFrom>\n" + "                            <owl:Restriction>\n"
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>\n"
			+ "                                <owl:someValuesFrom>\n"
			+ "                                    <owl:Restriction>\n"
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>\n"
			+ "                                        <owl:someValuesFrom>\n"
			+ "                                            <rdfs:Datatype>\n"
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>\n"
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\">\n"
			+ "                                                    <rdf:Description>\n"
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">40.0</xsd:minExclusive>\n"
			+ "                                                    </rdf:Description>\n"
			+ "                                                </owl:withRestrictions>\n"
			+ "                                            </rdfs:Datatype>\n"
			+ "                                        </owl:someValuesFrom>\n"
			+ "                                    </owl:Restriction>\n"
			+ "                                </owl:someValuesFrom>\n"
			+ "                            </owl:Restriction>\n" + "                        </owl:someValuesFrom>\n"
			+ "                    </owl:Restriction>\n" + "                    <owl:Restriction>\n"
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>\n"
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n"
			+ "                    </owl:Restriction>\n" + "                </owl:intersectionOf>\n"
			+ "            </owl:Class>\n" + "        </owl:equivalentClass>\n"
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    </owl:Class>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#StaffPresent -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\">\n"
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "    </owl:Class>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#Warning -->\n" + "\n"
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#Warning\"/>\n" + "    \n" + "\n"
			+ "\n" + "    <!-- \n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "    //\n" + "    // Individuals\n" + "    //\n"
			+ "    ///////////////////////////////////////////////////////////////////////////////////////\n"
			+ "     -->\n" + "\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#lightIntensity -->\n" + "\n"
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\">\n"
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>\n"
			+ "    </owl:NamedIndividual>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#motionIntensity -->\n" + "\n"
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#motionIntensity\">\n"
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>\n"
			+ "    </owl:NamedIndividual>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity -->\n" + "\n" +

			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\">\n"
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n"
			+ "    </owl:NamedIndividual>\n" + "</rdf:RDF>\n" + "\n" + "\n" + "\n"
			+ "<!-- Generated by the OWL API (version 4.2.8.20170104-2310) https://github.com/owlcs/owlapi -->\n" + "\n"
			+ "";
}
