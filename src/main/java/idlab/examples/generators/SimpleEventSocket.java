package idlab.examples.generators;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.StatusCode;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketError;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

@WebSocket(maxTextMessageSize = 64 * 1024)
public class SimpleEventSocket {
	private final CountDownLatch closeLatch;
	@SuppressWarnings("unused")
	private Session session;

	public SimpleEventSocket() {
		this.closeLatch = new CountDownLatch(1);
	}

	public boolean awaitClose(int duration, TimeUnit unit) throws InterruptedException {
		return this.closeLatch.await(duration, unit);
	}

	@OnWebSocketClose
	public void onClose(int statusCode, String reason) {

	}

	@OnWebSocketConnect
	public void onConnect(Session session) {
		try {
			System.out.println("trying to send:");
			for (int i = 0; i < 10; i++) {
				session.getRemote().sendString(EVENT);
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}

	@OnWebSocketMessage
	public void onMessage(String msg) {
		System.out.println("received");

	}

	@OnWebSocketError
	public void onError(Throwable cause) {
		System.out.print("WebSocket Error: ");
		cause.printStackTrace(System.out);
	}

	private static String ONT_EVENT = "<?xml version=\"1.0\"?>\n"
			+ "<rdf:RDF xmlns=\"http://IBCNServices.github.io/homelabPlus.owl#\"\n"
			+ "     xml:base=\"http://IBCNServices.github.io/homelabPlus.owl\"\n"
			+ "     xmlns:owl=\"http://www.w3.org/2002/07/owl#\"\n"
			+ "     xmlns:rdf=\"http://www.w3.org/1999/02/22-rdf-syntax-ns#\"\n"
			+ "     xmlns:ssn=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#\"\n"
			+ "     xmlns:xml=\"http://www.w3.org/XML/1998/namespace\"\n"
			+ "     xmlns:xsd=\"http://www.w3.org/2001/XMLSchema#\"\n"
			+ "     xmlns:rdfs=\"http://www.w3.org/2000/01/rdf-schema#\">\n"
			+ "    <owl:Ontology rdf:about=\"http://IBCNServices.github.io/homelabPlus2.owl\">\n"
			+ "        <owl:imports rdf:resource=\"http://IBCNServices.github.io/homelabPlus.owl\"/>\n"
			+ "    </owl:Ontology>\n" + "    \n" + "\n" + "\n" + "    <!-- \n"
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
			+ "    <!-- http://IBCNServices.github.io/homelab.owl#soundIntensity -->\n" + "\n"
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelab.owl#soundIntensity\">\n"
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/SSNiot#Sound\"/>\n"
			+ "    </owl:NamedIndividual>\n" + "    \n" + "\n" + "\n"
			+ "    <!-- http://IBCNServices.github.io/homelabPlus.owl#obs -->\n" + "\n"
			+ "    <owl:NamedIndividual rdf:about=\"http://IBCNServices.github.io/homelabPlus.owl#obs\">\n"
			+ "        <rdf:type rdf:resource=\"http://IBCNServices.github.io/Accio-Ontology/ontologies/ssn#Observation\"/>\n"
			+ "        <ssn:observedProperty rdf:resource=\"http://IBCNServices.github.io/homelab.owl#lightIntensity\"/>\n"
			+ "    </owl:NamedIndividual>\n" + "</rdf:RDF>";
	
	private static String EVENT = "@prefix rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#> .\n"
			+ "@prefix foaf: <http://xmlns.com/foaf/0.1/> .\n"
			+ "@prefix td: <http://purl.org/td/transportdisruption#> .\n"
			+ "@prefix dct: <http://purl.org/dc/terms/> .\n" + "@prefix gtfs: <http://vocab.gtfs.org/terms#> .\n"
			+ "@prefix ex: <http://example.com/> .\n" + "\n" + "_:b12_0 rdf:type td:PublicTransportDiversion ;\n"
			+ "	dct:description \"Door wegenwerken rijdt lijn 6, de rit van 10.50 u. uit Oostende Station & de rit van 11.08 uur uit Raversijde Middenlaan niet\"@nl ;\n"
			+ "	td:factor td:RoadWorks ;\n"
			+ "	ex:hasHalte <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501331>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501375>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501372>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501348>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501290>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/510003>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501300>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506290>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506286>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501326>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501262>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501307>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506291>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506307>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506262>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506372>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501345>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501286>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506337>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506289>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506330>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506375>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/510004>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506326>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501373>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506300>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501289>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506345>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506374>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/506348>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501291>, <https://api.delijn.be/DLKernOpenData/v1/haltes/5/501337> ;\n"
			+ "	ex:hasBusLine <https://api.delijn.be/DLKernOpenData/v1/lijnen/5/906/lijnrichtingen/HEEN>, <https://api.delijn.be/DLKernOpenData/v1/lijnen/5/906/lijnrichtingen/TERUG> .\n"
			+ "\n" + "_:b12_1 rdf:type td:PublicTransportDiversion ;\n"
			+ "	dct:description \"Door weersomstandigheden is de dienstregeling van alle streeklijnen ernstig verstoord * Vermoedelijk hinder 14:00u tot 19.00u\"@nl ;\n"
			+ "	td:factor td:BadWeather ;\n"
			+ "	ex:hasHalte <https://api.delijn.be/DLKernOpenData/v1/haltes/5/109744> .\n" + "\n"
			+ "_:b12_2 rdf:type td:PublicTransportDiversion ;\n"
			+ "	dct:description \"Door weersomstandigheden is de dienstregeling van alle stadslijnen ernstig verstoord. vermoedelijke hinder tussen 12 en 20 uur\"@nl ;\n"
			+ "	td:factor td:BadWeather ;\n"
			+ "	ex:hasHalte <https://api.delijn.be/DLKernOpenData/v1/haltes/5/109744> .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501331> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501331\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501331\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501375> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501375\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501375\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501372> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501372\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501372\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501348> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501348\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501348\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501290> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501290\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501290\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/510003> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"510003\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/510003\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501300> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501300\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501300\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506290> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506290\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506290\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506286> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506286\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506286\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501326> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501326\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501326\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501262> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501262\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501262\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501307> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501307\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501307\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506291> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506291\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506291\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506307> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506307\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506307\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506262> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506262\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506262\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506372> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506372\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506372\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501345> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501345\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501345\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501286> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501286\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501286\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506337> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506337\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506337\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506289> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506289\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506289\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506330> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506330\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506330\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506375> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506375\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506375\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/510004> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"510004\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/510004\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506326> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506326\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506326\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501373> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501373\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501373\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506300> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506300\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506300\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501289> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501289\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501289\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506345> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506345\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506345\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506374> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506374\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506374\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/506348> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"506348\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/506348\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501291> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501291\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501291\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/501337> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"501337\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/501337\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/haltes/5/109744> rdf:type gtfs:Stop ;\n"
			+ "	gtfs:code \"109744\" ;\n" + "	foaf:page \"https://www.delijn.be/nl/haltes/halte/109744\" .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/lijnen/5/906/lijnrichtingen/HEEN> rdf:type gtfs:Route ;\n"
			+ "	dct:description \"Oostende Station-Luchthaven Raversijde\"@nl .\n" + "\n"
			+ "<https://api.delijn.be/DLKernOpenData/v1/lijnen/5/906/lijnrichtingen/TERUG> rdf:type gtfs:Route ;\n"
			+ "	dct:description \"Luchthaven via Raversijde-Oostende Station\"@nl .\n" + "\n" + "";
}