package idlab.massif.pipeline;

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
import idlab.massif.sources.KafkaSource;
import idlab.massif.window.esper.EsperWindow;

public class KafkaTest {
	@Test
	public void test() throws InterruptedException, OWLOntologyCreationException {
		//define window
		WindowInf window = new EsperWindow();
		window.setWindowSize(1);
		window.start();
		
		//define filter
		String query = "PREFIX : <http://streamreasoning.org/iminds/massif/> " 
				+ "CONSTRUCT{?work ?pred ?type.} "
		
				+ "WHERE  {  ?work ?pred ?type. }";
		FilterInf filter = new JenaFilter();
		int filterQueryID=filter.registerContinuousQuery(query);
		filter.start();
		//define the abstraction layer
		
		//define sink component
		SinkInf printSink = new PrintSink();
		
		
		PipeLineComponent sinkComp = new PipeLineComponent(printSink,Collections.EMPTY_LIST);
		
		
		PipeLineComponent filterComp = new PipeLineComponent(filter,Collections.singletonList(sinkComp));

		PipeLineComponent windowComp = new PipeLineComponent(window,Collections.singletonList(filterComp));
		
//		for(int i = 0;i<10;i++) {
//			windowComp.addEvent(ONT_EVENTa);
//			Thread.sleep(1000);
//			windowComp.addEvent(ONT_EVENTb);
//
//		}
		KafkaSource kafkaSource = new KafkaSource("kafka-headless.kafka:9092","idlab.homelab");
		kafkaSource = new KafkaSource("localhost:9092","semantic-input");

		PipeLineComponent sourceComp = new PipeLineComponent(kafkaSource,Collections.singletonList(windowComp));
		kafkaSource.stream();
		Thread.sleep(1000000);
		
	}

}
