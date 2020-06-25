package idlab.massif;

import org.junit.Test;
import org.semanticweb.owlapi.model.OWLOntologyCreationException;

import idlab.massif.core.PipeLine;
import idlab.massif.core.PipeLineGraph;
import idlab.massif.interfaces.core.SelectionInf;
import idlab.massif.run.QueryParser;

public class QueryTest {
	
	@Test
	public void test() {
		System.out.println(ONT_EVENT);
		String query = "{\"ontology\":\""+ONT_STRING.replace("\"", "\\\"")+"\","
				
				+"\"classExpression\": [{\"tail\":\"Observation and (observedProperty some LightIntensity)\",\"head\":\"http://massif.test/EventA\"}],"
				+"\"cep\":[{\"query\":\"a=EventA or b=EventB\",\"types\":[\"EventA\",\"EventB\"]}],"
				+"\"sparql\":\"PREFIX : <http://streamreasoning.org/iminds/massif/> CONSTRUCT{?work ?pred ?type.} WHERE  {  ?work ?pred ?type. }\","
				+"\"source\":[{\"type\":\"POST\",\"port\":9090}]"
				+ "}";
		
		//System.out.println(query);
		String query2 = "{\"components\":{\"comp1\":{\"type\":\"Sink\",\"impl\":\"PrintSink\"},\"comp2\":{\"type\":\"window\",\"size\":5,\"slide\":1},comp3:{\"type\":\"Filter\",\"queries\":[]},\"comp4\":{\"type\":\"Abstract\",\"expressions\":[{\"head\":\"http://massif.test/EventA\",\"tail\":\"Observation and (observedProperty some LightIntensity)\"}]},\"comp5\":{\"type\":\"Source\",\"impl\":\"KafkaSource\",\"kafkaServer\":\"localhost:9092\",\"kafkaTopic\":\"semantic-input\"}}, \"configuration\":{\"comp1\":[],\"comp2\":[\"comp3\"],\"comp3\":[\"comp1\"]}}";
		String query3="{\n" + 
				"  \"components\": {\n" + 
				"    \"comp1\": {\n" + 
				"      \"type\": \"Sink\",\n" + 
				"      \"impl\": \"PrintSink\"\n" + 
				"    },\n" + 
				"    \"comp2\": {\n" + 
				"      \"type\": \"window\",\n" + 
				"      \"size\": 1,\n" + 
				"      \"slide\": 1\n" + 
				"    },\n" + 
				"    \"comp3\": {\n" + 
				"      \"type\": \"Filter\",\n" + 
				"      \"queries\": [\"CONSTRUCT{?s ?p ?p.} WHERE {?s ?p ?o}\"]\n" + 
				"    },\n" + 
				"    \"comp4\": {\n" + 
				"      \"type\": \"Abstract\",\n" + 
				"      \"expressions\": [\n" + 
				"        {\n" + 
				"          \"head\": \"http://massif.test/EventA\",\n" + 
				"          \"tail\": \"Observation\"\n" + 
				"        }\n" + 
				"      ]\n" + 
				"    },\n" + 
				"    \"comp5\": {\n" + 
				"      \"type\": \"Source\",\n" + 
				"      \"impl\": \"HTTPPostSource\",\n" + 
				"      \"path\": \"send\",\n" + 
				"      \"port\": 9000\n" + 
				"    }\n" + 
				"  },\n" + 
				"  \"configuration\": {\n" + 
				"    \"comp1\": [],\n" + 
				"    \"comp3\": [\n" + 
				"      \"comp4\"\n" + 
				"    ],\n" + 
				"    \"comp2\": [\n" + 
				"      \"comp3\"\n" + 
				"    ],\n" + 
				"    \"comp5\": [\n" + 
				"      \"comp2\"\n" + 
				"    ],\n" + 
				"    \"comp4\": [\n" + 
				"      \"comp1\"\n" + 
				"    ]\n" + 
				"  }\n" + 
				"}";
		QueryParser parser = new QueryParser();
		try {
			PipeLineGraph engine = parser.parse(query3);
			
			
		} catch (OWLOntologyCreationException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	private static String ONT_EVENT="<?xml version=\"1.0\"?>\n" + 
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
			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#obs\">\n" + 
			"        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n" + 
			"        <ssn:observedProperty rdf:resource=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\"/>\n" + 
			"    </owl:NamedIndividual>\n" + 
			"</rdf:RDF>";
	private static String ONT_STRING = "<?xml version=\"1.0\"?> "
			+ "<rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\" "
			+ "     xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\" "
			+ "     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\" "
			+ "     xmlns:owl=\"http://www.w3.org/2002/07/owl#\" "
			+ "     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\" "
			+ "     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\" "
			+ "     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\"> "
			+ "    <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl\"/> " + "     " + " "
			+ " " + "    <!--  "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "    // " + "    // Object Properties " + "    // "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "     --> " + " " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasLocation\"> "
			+ "        <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/> "
			+ "    </owl:ObjectProperty> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasRole\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#isLocationOf\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedBy\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/> "
			+ "     " + " " + " " + "    <!-- http://IBCNServices.github.io/homelab.owl#belongsTo --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#belongsTo\"> "
			+ "        <owl:inverseOf rdf:resource=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/> "
			+ "    </owl:ObjectProperty> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#isLinkedTo --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#isLinkedTo\"/> "
			+ "     " + " " + " " + "    <!-- http://IBCNServices.github.io/homelab.owl#updates --> " + " "
			+ "    <owl:ObjectProperty rdf:about=\"http://IBCNServices.github.io/homelab.owl#updates\"/> " + "     "
			+ " " + " " + "    <!--  "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "    // " + "    // Data properties " + "    // "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "     --> " + " " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue --> " + " "
			+ "    <owl:DatatypeProperty rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/> "
			+ "     " + " " + " " + "    <!--  "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "    // " + "    // Classes " + "    // "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "     --> " + " " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/> "
			+ "     " + " " + " " + "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion --> "
			+ " " + "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/> "
			+ "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#PersonDetected\"> "
			+ "        <owl:equivalentClass rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\"/> "
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "    </owl:Class> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/> " + "     "
			+ " " + " " + "    <!-- http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation --> "
			+ " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "     " + " " + " " + "    <!-- http://IBCNServices.github.io/homelab.owl#ActuatorUpdate --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#ActuatorUpdate\"/> " + "     "
			+ " " + " " + "    <!-- http://IBCNServices.github.io/homelab.owl#Patient --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#Patient\"/> " + "     " + " "
			+ " " + "    <!-- http://IBCNServices.github.io/homelab.owl#StaffMember --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelab.owl#StaffMember\"/> " + "     " + " "
			+ " " + "    <!-- http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#LightThresholdObservation\"> "
			+ "        <owl:equivalentClass> " + "            <owl:Class> "
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\"> "
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/> "
			+ "                        <owl:someValuesFrom> " + "                            <owl:Restriction> "
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/> "
			+ "                                <owl:someValuesFrom> "
			+ "                                    <owl:Restriction> "
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/> "
			+ "                                        <owl:someValuesFrom> "
			+ "                                            <rdfs:Datatype> "
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/> "
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\"> "
			+ "                                                    <rdf:Description> "
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">30.0</xsd:minExclusive> "
			+ "                                                    </rdf:Description> "
			+ "                                                </owl:withRestrictions> "
			+ "                                            </rdfs:Datatype> "
			+ "                                        </owl:someValuesFrom> "
			+ "                                    </owl:Restriction> "
			+ "                                </owl:someValuesFrom> "
			+ "                            </owl:Restriction> " + "                        </owl:someValuesFrom> "
			+ "                    </owl:Restriction> " + "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/> "
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/> "
			+ "                    </owl:Restriction> " + "                </owl:intersectionOf> "
			+ "            </owl:Class> " + "        </owl:equivalentClass> "
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "    </owl:Class> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#MotionThresholdObservation\"> "
			+ "        <owl:equivalentClass> " + "            <owl:Class> "
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\"> "
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/> "
			+ "                        <owl:someValuesFrom> " + "                            <owl:Restriction> "
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/> "
			+ "                                <owl:someValuesFrom> "
			+ "                                    <owl:Restriction> "
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/> "
			+ "                                        <owl:someValuesFrom> "
			+ "                                            <rdfs:Datatype> "
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/> "
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\"> "
			+ "                                                    <rdf:Description> "
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">0.0</xsd:minExclusive> "
			+ "                                                    </rdf:Description> "
			+ "                                                </owl:withRestrictions> "
			+ "                                            </rdfs:Datatype> "
			+ "                                        </owl:someValuesFrom> "
			+ "                                    </owl:Restriction> "
			+ "                                </owl:someValuesFrom> "
			+ "                            </owl:Restriction> " + "                        </owl:someValuesFrom> "
			+ "                    </owl:Restriction> " + "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/> "
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/> "
			+ "                    </owl:Restriction> " + "                </owl:intersectionOf> "
			+ "            </owl:Class> " + "        </owl:equivalentClass> "
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "    </owl:Class> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#SoundThresholdObservation\"> "
			+ "        <owl:equivalentClass> " + "            <owl:Class> "
			+ "                <owl:intersectionOf rdf:parseType=\"Collection\"> "
			+ "                    <rdf:Description rdf:about=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observationResult\"/> "
			+ "                        <owl:someValuesFrom> " + "                            <owl:Restriction> "
			+ "                                <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#hasValue\"/> "
			+ "                                <owl:someValuesFrom> "
			+ "                                    <owl:Restriction> "
			+ "                                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/DUL.owl#hasDataValue\"/> "
			+ "                                        <owl:someValuesFrom> "
			+ "                                            <rdfs:Datatype> "
			+ "                                                <owl:onDatatype rdf:resource=\"http://www.w3.org/2001/XMLSchema#double\"/> "
			+ "                                                <owl:withRestrictions rdf:parseType=\"Collection\"> "
			+ "                                                    <rdf:Description> "
			+ "                                                        <xsd:minExclusive rdf:datatype=\"http://www.w3.org/2001/XMLSchema#double\">40.0</xsd:minExclusive> "
			+ "                                                    </rdf:Description> "
			+ "                                                </owl:withRestrictions> "
			+ "                                            </rdfs:Datatype> "
			+ "                                        </owl:someValuesFrom> "
			+ "                                    </owl:Restriction> "
			+ "                                </owl:someValuesFrom> "
			+ "                            </owl:Restriction> " + "                        </owl:someValuesFrom> "
			+ "                    </owl:Restriction> " + "                    <owl:Restriction> "
			+ "                        <owl:onProperty rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#observedProperty\"/> "
			+ "                        <owl:someValuesFrom rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/> "
			+ "                    </owl:Restriction> " + "                </owl:intersectionOf> "
			+ "            </owl:Class> " + "        </owl:equivalentClass> "
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "    </owl:Class> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#StaffPresent --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#StaffPresent\"> "
			+ "        <rdfs:subClassOf rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/> "
			+ "    </owl:Class> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#Warning --> " + " "
			+ "    <owl:Class rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#Warning\"/> " + "     " + " "
			+ " " + "    <!--  "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "    // " + "    // Individuals " + "    // "
			+ "    /////////////////////////////////////////////////////////////////////////////////////// "
			+ "     --> " + " " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#lightIntensity --> " + " "
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\"> "
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#LightIntensity\"/> "
			+ "    </owl:NamedIndividual> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#motionIntensity --> " + " "
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#motionIntensity\"> "
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Motion\"/> "
			+ "    </owl:NamedIndividual> " + "     " + " " + " "
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity --> " + " " +

			"    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\"> "
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/> "
			+ "    </owl:NamedIndividual> " + "</rdf:RDF> " + " " + " " + " "
			+ "<!-- Generated by the OWL API (version 4.2.8.20170104-2310) https://github.com/owlcs/owlapi --> " + " "
			+ "";
}
