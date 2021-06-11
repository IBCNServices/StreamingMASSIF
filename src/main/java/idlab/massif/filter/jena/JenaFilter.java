package idlab.massif.filter.jena;

import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.InfModel;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.reasoner.Reasoner;
import org.apache.jena.reasoner.rulesys.GenericRuleReasoner;
import org.apache.jena.reasoner.rulesys.Rule;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import idlab.massif.interfaces.core.FilterInf;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.utils.FormatUtils;
import idlab.massif.utils.RDFUtils;

public class JenaFilter implements FilterInf {

    protected Model infModel;
    protected List<Query> queries;
    protected List<String> queryStrings;
    private ListenerInf listener;
    protected String rules;
    protected List<String> dataSources;
    private final Logger logger = LoggerFactory.getLogger(this.getClass());

    public JenaFilter() {
        this.infModel = ModelFactory.createDefaultModel();
        this.queries = new ArrayList<Query>();
        this.queryStrings = new ArrayList<String>();
        this.dataSources = new ArrayList<String>();
    }


    @Override
    public boolean addEvent(String event) {
        logger.debug("Received message: " + event);
        // add the data
        try {

            List<Statement> statements = RDFUtils.parseTriples(event);

            infModel.add(statements);
            System.out.println("Query model size: " + infModel.size());

            // execute the query
            int queryId = 0;
            for (Query query : queries) {

                try (QueryExecution qexec = QueryExecutionFactory.create(query, infModel)) {

                    if (!query.isSelectType()) {
                        // check if quad construct or normal construct
                        if (query.isConstructQuad()) {
                            Dataset results = qexec.execConstructDataset();
                            Iterator<String> names = results.listNames();
                            while (names.hasNext()) {
                                String name = names.next();
                                Model m = results.getNamedModel(name);
                                String resultString = RDFUtils.modelToString(m);
                                // notify the listener
                                notifyListener(queryId, resultString);

                            }

                        } else {
                            Model result = qexec.execConstruct();
                            if (!result.isEmpty()) {
                                String resultString = RDFUtils.modelToString(result);
                                // notify the listener
                                notifyListener(queryId, resultString);
                            }
                        }
                    } else {

                        ResultSet results = qexec.execSelect();
                        StringBuilder builder = new StringBuilder();
                        List<String> vars = results.getResultVars();
                        for (String var : vars) {

                            builder.append(var).append(",");
                        }
                        builder.append("\n");
                        for (; results.hasNext(); ) {
                            QuerySolution soln = results.nextSolution();
                            for (String var : vars) {
                                builder.append(soln.get(var)).append(",");
                            }
                            builder.append("\n");

                        }
                        notifyListener(queryId, builder.toString());
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

    private void notifyListener(int queryID, String result) {
        if (listener != null) {
            listener.notify(queryID, result);
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
            queryStrings = new ArrayList<String>();
        }
        Query query = QueryFactory.create(queryString, Syntax.syntaxARQ);
//		if (!query.isConstructType()) {
//			logger.error("Only construct queries allowed");
//			return -1;
//		}
        queries.add(query);
        queryStrings.add(queryString);
        return queries.size() - 1;
    }

    @Override
    public boolean setStaticData(String dataSource) {
        this.dataSources.add(dataSource);
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
            for (String dataSource : dataSources) {
                if (!dataSource.isEmpty() && dataSource.endsWith("sparql")) {
                    // fetch remote data
                    Model remote = getRemoteData(dataSource);
                    System.out.println("Remote size: " + remote.size());
                    this.infModel.add(remote);
                } else if (!dataSource.isEmpty()) {
                    this.infModel.read(dataSource, "TTL");
                    System.out.println("local size: " + infModel.size());
                }
            }
            if (rules != null) {

                Reasoner reasoner = new GenericRuleReasoner(Rule.rulesFromURL(rules));
                InfModel model = ModelFactory.createInfModel(reasoner, this.infModel);
                model.prepare();
                this.infModel = model;
                System.out.println("With rules size: " + infModel.size());
            }
            System.out.println("final size: " + infModel.size());
        } catch (Exception e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    private Model getRemoteData(String dataSource) {
        QueryExecution q = QueryExecutionFactory.sparqlService(dataSource, "CONSTRUCT {?s ?p ?o } WHERE {?s ?p ?o .} ");
        Model result = q.execConstruct();
        return result;
    }

    @Override
    public void stop() {
        // TODO Auto-generated method stub
        this.infModel.removeAll();
    }

    @Override
    public String toString() {
        StringBuilder str = new StringBuilder();

        str.append("{\"type\":\"Filter\",\"impl\":\"jena\",\"ontology\":\"" + String.join(",", this.dataSources)
                + "\",\"queries\":[");
        for (int i = 0; i < queryStrings.size(); i++) {
            str.append("\"").append(
                    FormatUtils.encodeQuery(queryStrings.get(i).toString()))
                    .append("\"");
            if (i < queryStrings.size() - 1) {
                str.append(",");
            }
        }
        str.append("]}");
        return str.toString();
    }

}
