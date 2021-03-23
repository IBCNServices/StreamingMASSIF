package idlab.massif.run;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.semanticweb.owlapi.apibinding.OWLManager;
import org.semanticweb.owlapi.io.StringDocumentSource;
import org.semanticweb.owlapi.model.OWLOntology;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;
import org.semanticweb.owlapi.model.OWLOntologyManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idlab.massif.abstraction.hermit.HermitAbstractionImpl;
import idlab.massif.cep.esper.EsperCEPImpl;
import idlab.massif.core.PipeLine;
import idlab.massif.core.PipeLineGraph;
import idlab.massif.exceptions.QueryRegistrationException;
import idlab.massif.interfaces.core.AbstractionInf;
import idlab.massif.interfaces.core.AbstractionListenerInf;
import idlab.massif.interfaces.core.CEPInf;
import idlab.massif.interfaces.core.CEPListener;
import idlab.massif.interfaces.core.SelectionInf;
import idlab.massif.interfaces.core.SelectionListenerInf;
import idlab.massif.selection.csparql_basic.CSparqlSelectionImpl;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import spark.Response;
import spark.Spark;

import static spark.Spark.*;
import org.json.JSONObject;

@Command(name = "MASSIF", mixinStandardHelpOptions = true, version = "Streaming MASSIF 0.1")
public class Run implements Runnable{
	Logger logger = LoggerFactory.getLogger(this.getClass().getName());
	
	Map<String,PipeLineGraph> configs;
	int configCounter = 0;
	private String sinks = "[]";
	public static void main(String[] args) throws Exception {
		int exitCode = new CommandLine(new Run()).execute(args);
        System.exit(exitCode);
		
		//new Run();
	}

	private PipeLine engine;
	private QueryParser parser;
	@Option(names = "-p", description = "Set port number, default: ${DEFAULT-VALUE}.")
	private int port = 9000;
	
	public Run() {
		logger.info("MASSIF STARTING");
		configs = new HashMap<String,PipeLineGraph>();
		parser =  new QueryParser();
		
        
	}
	public static void resetRoutes() {
		Spark.stop();
	}
	public void setSinks(String sinks) {
		this.sinks = sinks;
	}
	private String getSinks() {
		// TODO Auto-generated method stub
		return sinks;
	}
	public String stop(String queryID) {
		PipeLineGraph g = configs.get(queryID);
		g.stop();
		configs.remove(queryID);
		logger.info("Deactivated QueryID {}",queryID);
		return "ok";
	}
	public void stopPrevious() {
		logger.debug("Stopping all configurations");
		new HashSet<String>(configs.keySet()).forEach(k-> this.stop(k));
	}
	public  String register(String query,Response response) {
		logger.info("Registering new Query {}",query);
		stopPrevious();
		
		try {
			PipeLineGraph graph = parser.parse(query);
			String id = ++configCounter+"";
			configs.put(id, graph);
			response.status(200);
			return id;
		} catch (QueryRegistrationException e) {
			// TODO Auto-generated catch block
			logger.error("Unable to register Query {}",query,e);
			response.status(400);
			return e.getMessage();
		}
		

	}

	public void run()  {
		logger.info(String.format("MASSIF Listening on port %s", port));
		logger.info(String.format("Access the MASSIF GUI on  localhost:%s "
				+ "or register a configuration on localhost:%s/register", port, port));
		port(port);
		staticFileLocation("/web");
        get("/hello", (req, res) -> "MASSIF ONLINE");
        
        post("/register",(req, res) ->  register(req.body(),res));
        post("/stop",(req, res) ->  stop(req.body()));
        get("/configs", (req, res) -> generateConfigs());
        get("/sinks", (req, res) -> getSinks());
        //post("/send",(req, res) -> {engine.addEvent(req.body()); return res.status();});
        logger.info("MASSIF is ONLINE");
        keepAlive();
	}
	private void keepAlive() {
		while(true) {
        	try {
				Thread.sleep(100000);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
        }
	}
	public String generateConfigs() {
		StringBuilder str = new StringBuilder();
		str.append("{");
		for (Entry<String,PipeLineGraph> entry:configs.entrySet()) {
			str.append("\"").append(entry.getKey()).append("\":").append(entry.getValue().toString()).append(",");
		}
		str.append("}");
		String result = str.toString();
		System.out.println(result);
		return result;
	}
	public void registerGenericComponent(String parseName,Class clazz){
		parser.registerGenericComponent(parseName, clazz);
	}

	
}
//{"ontology":"<?xml version=\"1.0\"?> <rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\"      xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\"      xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"      xmlns:owl=\"http://www.w3.org/2002/07/owl#\"      xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"      xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"      xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">     <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl\"/>            <!--      ///////////////////////////////////////////////////////////////////////////////////////     //     // Object Properties     //     ///////////////////////////////////////////////////////////////////////////////////////      -->             <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation\">         <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/>     </owl:ObjectProperty>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>            <!-- http://IBCNServices.github.io/homelab.owl#belongsTo -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#belongsTo\">         <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/>     </owl:ObjectProperty>            <!-- http://IBCNServices.github.io/homelab.owl#isLinkedTo -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/>            <!-- http://IBCNServices.github.io/homelab.owl#updates -->      <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#updates\"/>            <!--      ///////////////////////////////////////////////////////////////////////////////////////     //     // Data properties     //     ///////////////////////////////////////////////////////////////////////////////////////      -->             <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue -->      <owl:DatatypeProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>            <!--      ///////////////////////////////////////////////////////////////////////////////////////     //     // Classes     //     ///////////////////////////////////////////////////////////////////////////////////////      -->             <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected\">         <owl:equivalentClass rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\"/>         <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>     </owl:Class>            <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>            <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>            <!-- http://IBCNServices.github.io/homelab.owl#ActuatorUpdate -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#ActuatorUpdate\"/>            <!-- http://IBCNServices.github.io/homelab.owl#Patient -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#Patient\"/>            <!-- http://IBCNServices.github.io/homelab.owl#StaffMember -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#StaffMember\"/>            <!-- http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation\">         <owl:equivalentClass>             <owl:Class>                 <owl:intersectionOf rdf:parseType=\"Collection\">                     <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>                         <owl:someValuesFrom>                             <owl:Restriction>                                 <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>                                 <owl:someValuesFrom>                                     <owl:Restriction>                                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>                                         <owl:someValuesFrom>                                             <rdfs:Datatype>                                                 <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>                                                 <owl:withRestrictions rdf:parseType=\"Collection\">                                                     <rdf:Description>                                                         <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">30.0</xsd:minExclusive>                                                     </rdf:Description>                                                 </owl:withRestrictions>                                             </rdfs:Datatype>                                         </owl:someValuesFrom>                                     </owl:Restriction>                                 </owl:someValuesFrom>                             </owl:Restriction>                         </owl:someValuesFrom>                     </owl:Restriction>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>                         <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>                     </owl:Restriction>                 </owl:intersectionOf>             </owl:Class>         </owl:equivalentClass>         <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>     </owl:Class>            <!-- http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation\">         <owl:equivalentClass>             <owl:Class>                 <owl:intersectionOf rdf:parseType=\"Collection\">                     <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>                         <owl:someValuesFrom>                             <owl:Restriction>                                 <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>                                 <owl:someValuesFrom>                                     <owl:Restriction>                                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>                                         <owl:someValuesFrom>                                             <rdfs:Datatype>                                                 <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>                                                 <owl:withRestrictions rdf:parseType=\"Collection\">                                                     <rdf:Description>                                                         <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">0.0</xsd:minExclusive>                                                     </rdf:Description>                                                 </owl:withRestrictions>                                             </rdfs:Datatype>                                         </owl:someValuesFrom>                                     </owl:Restriction>                                 </owl:someValuesFrom>                             </owl:Restriction>                         </owl:someValuesFrom>                     </owl:Restriction>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>                         <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>                     </owl:Restriction>                 </owl:intersectionOf>             </owl:Class>         </owl:equivalentClass>         <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>     </owl:Class>            <!-- http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation\">         <owl:equivalentClass>             <owl:Class>                 <owl:intersectionOf rdf:parseType=\"Collection\">                     <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/>                         <owl:someValuesFrom>                             <owl:Restriction>                                 <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/>                                 <owl:someValuesFrom>                                     <owl:Restriction>                                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/>                                         <owl:someValuesFrom>                                             <rdfs:Datatype>                                                 <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/>                                                 <owl:withRestrictions rdf:parseType=\"Collection\">                                                     <rdf:Description>                                                         <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">40.0</xsd:minExclusive>                                                     </rdf:Description>                                                 </owl:withRestrictions>                                             </rdfs:Datatype>                                         </owl:someValuesFrom>                                     </owl:Restriction>                                 </owl:someValuesFrom>                             </owl:Restriction>                         </owl:someValuesFrom>                     </owl:Restriction>                     <owl:Restriction>                         <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/>                         <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>                     </owl:Restriction>                 </owl:intersectionOf>             </owl:Class>         </owl:equivalentClass>         <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>     </owl:Class>            <!-- http://IBCNServices.github.io/homelabPlus.owl#StaffPresent -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\">         <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>     </owl:Class>            <!-- http://IBCNServices.github.io/homelabPlus.owl#Warning -->      <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#Warning\"/>            <!--      ///////////////////////////////////////////////////////////////////////////////////////     //     // Individuals     //     ///////////////////////////////////////////////////////////////////////////////////////      -->             <!-- http://IBCNServices.github.io/homelab.owl#lightIntensity -->      <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\">         <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/>     </owl:NamedIndividual>            <!-- http://IBCNServices.github.io/homelab.owl#motionIntensity -->      <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#motionIntensity\">         <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/>     </owl:NamedIndividual>            <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity -->      <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\">         <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>     </owl:NamedIndividual> </rdf:RDF>    <!-- Generated by the OWL API (version 4.2.8.20170104-2310) https://github.com/owlcs/owlapi -->  ","classExpression": [{"tail":"Observation and (observedProperty some LightIntensity)","head":"http://massif.test/EventA"}],"cep":[{"query":"a=EventA or b=EventB","types":["EventA","EventB"]}],"sparql":"PREFIX : <http://streamreasoning.org/iminds/massif/> CONSTRUCT{?work ?pred ?type.} WHERE  {  ?work ?pred ?type. }","source":[{"type":"POST","port":9090}]}
