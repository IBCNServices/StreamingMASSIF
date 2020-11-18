package idlab.massif.filter.jena;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idlab.massif.interfaces.core.FilterInf;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SelectionListenerInf;

public class JenaFilter implements FilterInf {

	private Model infModel;
	private List<Query> queries;
	private ListenerInf listener;
	private String rules;
	private String dataSource;
	private final Logger logger = LoggerFactory.getLogger(this.getClass());

	public JenaFilter() {
		this.infModel = ModelFactory.createDefaultModel();

	}

	@Override
	public boolean addEvent(String event) {
		logger.debug("Received message: " + event);
		// add the data
		Model dataModel = ModelFactory.createDefaultModel();
		try {
			InputStream targetStream = new ByteArrayInputStream(event.getBytes());
			dataModel.read(targetStream, null, "TTL");
			StmtIterator it = dataModel.listStatements();
			List<Statement> statements = new ArrayList<Statement>();
			while (it.hasNext()) {
				statements.add(it.next());
			}
			infModel.add(statements);
			// execute the query
			int queryId = 0;
			for (Query query : queries) {

				try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel)) {

					if (!query.isSelectType()) {
						Model result = qexec.execConstruct();
						if (!result.isEmpty()) {
							String syntax = "TURTLE"; // also try "N-TRIPLE" and "TURTLE"
							StringWriter out = new StringWriter();
							result.write(out, syntax);
							String resultString = out.toString();
							// notify the listener
							listener.notify(queryId, resultString);
						}
					} else {
						ResultSet results = qexec.execSelect() ;
						
						String strResults="";
						List<String> vars = results.getResultVars();
						for(String var: vars) {
							
							strResults+=var + ",";
						}
						strResults+="\n";
					    for ( ; results.hasNext() ; )
					    {
					      QuerySolution soln = results.nextSolution() ;
					      for(String var: vars) {
					    	  strResults+=soln.get(var) +",";
					      }
					      strResults+="\n";
					      
					    }
					    listener.notify(queryId, strResults);
					}
				}
				queryId++;

			}

			// remove the data
			infModel.remove(statements);
			return true;

		} catch (Exception e) {
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
	public int registerContinuousQuery(String queryString) {
		if (queries == null) {
			queries = new ArrayList<Query>();
		}
		Query query = QueryFactory.create(queryString);
//		if (!query.isConstructType()) {
//			logger.error("Only construct queries allowed");
//			return -1;
//		}
		queries.add(query);
		return queries.size() - 1;
	}

	@Override
	public boolean setStaticData(String dataSource) {
		this.dataSource = dataSource;
		return false;
	}

	@Override
	public boolean setRules(String rules) {
		this.rules = rules;
		return true;
	}

	public void start() {
		this.infModel = ModelFactory.createDefaultModel();

		try {
			if (dataSource != null && !dataSource.isEmpty()) {
				this.infModel.read(dataSource, "TTL");
			}
			if (rules != null) {

				Reasoner reasoner = new GenericRuleReasoner(Rule.rulesFromURL(rules));
				InfModel model = ModelFactory.createInfModel(reasoner, this.infModel);
				model.prepare();
				this.infModel = model;
			}

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		this.infModel.removeAll();
	}
	@Override
	public String toString() {
		StringBuilder str = new StringBuilder(); 

    str.append("{\"type\":\"Filter\",\"impl\":\"jena\",\"ontology\":\"http://test\",\"queries\":[");
    for(int i = 0; i<queries.size();i++) {
    	str.append("\"").append(queries.get(i).toString().replace("\n", "\\n")).append("\"");
    	if(i<queries.size()-1) {
    		str.append(",");
    	}
    }
    str.append("]}}}");
		return str.toString();
	}

}
