package idlab.massif.functions;

import java.util.Collections;
import java.util.List;

import org.apache.jena.sparql.expr.ExprList;
import org.apache.jena.sparql.expr.NodeValue;
import org.apache.jena.sparql.function.Function;
import org.apache.jena.sparql.function.FunctionBase;
import org.apache.jena.sparql.function.FunctionFactory;
import org.apache.jena.sparql.function.FunctionRegistry;

import idlab.massif.core.PipeLineComponent;
import idlab.massif.filter.jena.JenaFilter;
import idlab.massif.sinks.PrintSink;

public class FunctionManyParTest extends FunctionBase {
	
	public static void main(String[] args) {
		
		
		FunctionRegistry.get().put("http://example.org/function#myFunction", FunctionManyParTest.class) ;
		PrintSink sink = new PrintSink();
		JenaFilter filter = new JenaFilter();
		filter.registerContinuousQuery("PREFIX f: <http://example.org/function#>\n" + 
				"Select ?s (f:myFunction(?s, ?o,?p,?s, ?o,?p) as ?t) WHERE {?s ?p ?o. }");
		PipeLineComponent filterComp = new PipeLineComponent(filter, Collections.emptyList());
		
		filterComp.setOutput(Collections.singletonList(new PipeLineComponent(sink, Collections.emptyList())));

		filter.start();
		filter.addEvent(input);

	}

	@Override
	public NodeValue exec(List<NodeValue> args) {
		// TODO Auto-generated method stub
		System.out.println(args);
		return NodeValue.FALSE;
	}

	@Override
	public void checkBuild(String uri, ExprList args) {
		// TODO Auto-generated method stub
		
	}
	static String input = "<http://example.com/properties/airquality/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/ObservableProperty> .\n" + 
			"<http://example.com/sensors/sensor1/> <http://www.w3.org/ns/sosa/observes> <http://example.com/airquality> .\n" + 
			"<http://example.com/sensors/sensor1/> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/Sensor> .\n" + 
			"<http://example.com/sensors/sensor1/events/1591782280622> <http://www.w3.org/ns/sosa/hasFeatureOfInterest> <http://example.com/features/airquality> .\n" + 
			"<http://example.com/sensors/sensor1/events/1591782280622> <http://www.w3.org/ns/sosa/observedProperty> <http://example.com/properties/airquality> .\n" + 
			"<http://example.com/sensors/sensor1/events/1591782280622> <http://www.w3.org/ns/sosa/madeBySensor> <http://example.com/sensors/sensor1> .\n" + 
			"<http://example.com/sensors/sensor1/events/1591782280622> <http://www.w3.org/1999/02/22-rdf-syntax-ns#type> <http://www.w3.org/ns/sosa/Observation> .\n" + 
			"<http://example.com/sensors/sensor1/events/1591782280622> <http://www.w3.org/ns/sosa/hasSimpleResult> \"465.92\" .";


}
