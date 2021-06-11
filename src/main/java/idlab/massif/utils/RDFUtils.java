package idlab.massif.utils;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RiotException;

public class RDFUtils {
	private static String[] syntaxes = new String[]{"TTL", "RDF/XML"};
	private static int currentSyntaxIndex = 0;
	public static List<Statement> parseTriples(String event){
		Model dataModel = ModelFactory.createDefaultModel();
		readAndtryVariousSyntaxes(dataModel,event);

		StmtIterator it = dataModel.listStatements();
		List<Statement> statements = new ArrayList<Statement>();
		while (it.hasNext()) {
			statements.add(it.next());
		}
		return statements;
	}
	private static void readAndtryVariousSyntaxes(Model dataModel, String event){
		if(currentSyntaxIndex < syntaxes.length) {
			InputStream targetStream = new ByteArrayInputStream(event.getBytes());
			try {
				dataModel.read(targetStream, null, syntaxes[currentSyntaxIndex]);
			} catch (RiotException e) {
				currentSyntaxIndex++;
				readAndtryVariousSyntaxes(dataModel,event);
			}
		}else {
			currentSyntaxIndex = 0;
		}
	}
	public static String modelToString(Model m) {
		String syntax = "TURTLE"; // also try "N-TRIPLE" and "TURTLE"
		StringWriter out = new StringWriter();
		m.write(out, syntax);
		return out.toString();
		
	}
	public static String RDFStatementsToString(List<Statement> statements) {
		Model dataModel = ModelFactory.createDefaultModel();
		dataModel.add(statements);
		return modelToString(dataModel);
	}

}
