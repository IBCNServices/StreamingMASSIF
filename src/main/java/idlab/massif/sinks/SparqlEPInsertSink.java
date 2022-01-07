package idlab.massif.sinks;
// Initial release: L. Tailhardat - 2021.06.15

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SinkInf;

// DOC: https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/update/UpdateExecutionFactory.html
// DOC: https://jena.apache.org/documentation/javadoc/arq/org/apache/jena/update/UpdateFactory.html

import org.apache.jena.update.UpdateRequest;
import org.apache.jena.update.UpdateFactory;
import org.apache.jena.update.UpdateExecutionFactory;
import org.apache.jena.update.UpdateProcessor;
import org.apache.jena.update.UpdateException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparqlEPInsertSink implements SinkInf {

	private String serviceUrl;
	private String graphName;

	private static final Logger logger = LoggerFactory.getLogger(SparqlEPInsertSink.class);

	public SparqlEPInsertSink(String serviceUrl, String graphName) {
		this.serviceUrl = serviceUrl;
		this.graphName = graphName;
	}

	@Override
	public boolean addEvent(String event) {
		try {

			// TODO: send to debug
			logger.debug(
					"Received event: {}",
					event);

			String query = String.format(
					"INSERT DATA { GRAPH %s { %s } }",
					graphName,
					event);

			UpdateRequest ur = UpdateFactory.create(query);
			UpdateProcessor up = UpdateExecutionFactory.createRemote(ur, serviceUrl);

			logger.debug(
					"Executing SPARQL UPDATE query: {}",
					query);
			up.execute();

		} catch (UpdateException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return true;
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
	public void stop() {
		// TODO Auto-generated method stub
		
	}

}
