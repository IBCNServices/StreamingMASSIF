package idlab.massif.window.esper;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.List;

import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;

import com.espertech.esper.client.Configuration;
import com.espertech.esper.client.EPAdministrator;
import com.espertech.esper.client.EPRuntime;
import com.espertech.esper.client.EPServiceProvider;
import com.espertech.esper.client.EPServiceProviderManager;
import com.espertech.esper.client.EPStatement;
import com.espertech.esper.client.EventBean;
import com.espertech.esper.client.UpdateListener;
import com.espertech.esper.client.deploy.EPDeploymentAdmin;
import com.espertech.esper.client.time.CurrentTimeEvent;

import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.WindowInf;
import idlab.massif.selection.csparql_basic.utils.GraphEvent;

public class EsperWindow implements WindowInf {

	private int windowSize;
	private int windowSlide;
	private EPRuntime cepRT;
	private ListenerInf listener;
	private int counter;
	private EPServiceProvider cep;
	@Override
	public boolean addEvent(String event) {
		this.advanceTime(System.currentTimeMillis());
		Model dataModel = ModelFactory.createDefaultModel();
		try {
			InputStream targetStream = new ByteArrayInputStream(event.getBytes());
			dataModel.read(targetStream,null,"TTL");
			StmtIterator it = dataModel.listStatements();
			List<Statement> statements = new ArrayList<Statement>();
			while (it.hasNext()) {
				statements.add(it.next());
			}
			this.addEvent(statements);
			return true;

		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		// TODO Auto-generated method stub
		this.listener=listener;
		return true;
	}

	@Override
	public void setWindowSize(int window) {
		this.windowSize=window;
		this.windowSlide=window;
		
	}

	@Override
	public void setWindowSize(int windowSize, int windowSlide) {
		this.windowSize=windowSize;
		this.windowSlide=windowSlide;
		
	}
	public void addEvent(List<Statement> event) {
		cepRT.sendEvent(new GraphEvent(counter++, event));
	}

	public void advanceTime(long time) {
		cepRT.sendEvent(new CurrentTimeEvent(time));
	}
	public void start() {
		Configuration cep_config = new Configuration();

		cep_config.getEngineDefaults().getThreading().setInternalTimerEnabled(false);
		cep_config.getEngineDefaults().getLogging().setEnableExecutionDebug(true);
		cep_config.getEngineDefaults().getLogging().setEnableTimerDebug(true);
		cep_config.getEngineDefaults().getLogging().setEnableQueryPlan(true);
		cep_config.getEngineDefaults().getMetricsReporting().setEnableMetricsReporting(true);
		cep_config.getEngineDefaults().getLogging().setEnableQueryPlan(true);

		this.cep = EPServiceProviderManager.getDefaultProvider(cep_config);
		this.cepRT = cep.getEPRuntime();
		EPAdministrator cepAdm = cep.getEPAdministrator();
		EPServiceProvider epService = EPServiceProviderManager.getDefaultProvider();
		EPDeploymentAdmin deployAdmin = cepAdm.getDeploymentAdmin();
		cep.getEPAdministrator().getConfiguration().addEventType(GraphEvent.class);
		
		String eplStatement = String.format("select * from GraphEvent#time(%s sec) output snapshot every %s seconds",
				windowSize, windowSlide);

		try {
			EPStatement statement = cepAdm.createEPL(eplStatement);

			statement.addListener(new UpdateListener() {
				public void update(EventBean[] newEvents, EventBean[] oldEvents) {
					if (newEvents != null) {
						try {
							
							List<Statement> add = new ArrayList<Statement>();
							for (EventBean e : newEvents) {
								add.addAll((List<Statement>) e.get("triples"));								

							}
							String event = convertToStringEvent(add);
							//notify the listener
							listener.notify(0,event);
							
						} catch (Exception e) {
							e.printStackTrace();
						}
					}

				}
			});
		} catch (Exception e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}
	}
	private String convertToStringEvent(List<Statement> statements) {
		Model result = ModelFactory.createDefaultModel();
		result.add(statements);
		String syntax = "TTL"; // also try "N-TRIPLE" and "TURTLE"
		StringWriter out = new StringWriter();
		result.write(out, syntax);
		String resultString = out.toString();
		return resultString;
	}

	@Override
	public void stop() {
		// TODO Auto-generated method stub
		this.cep.getEPAdministrator().destroyAllStatements();
	}

}
