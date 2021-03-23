package idlab.massif.sources;



import idlab.massif.core.PipeLine;
import idlab.massif.interfaces.core.ListenerInf;
import idlab.massif.interfaces.core.SourceInf;
import spark.Spark;

/***
 * Provides a Source that can be externally call through the provided url through a HTTP Post request.
 * 
 * @author psbonte
 *
 */
public class HTTPPostSource implements SourceInf {

	private String path;
	private PipeLine pipeline;
	private ListenerInf listener;
	private int port;

	public HTTPPostSource(String path,int port) {
		this.path = path;
		this.port = port;
	}

	public void registerPipeline(PipeLine pipeline) {
		this.pipeline = pipeline;
	}

	public void stream() {
		//Spark.port(this.port);
		Spark.post("/" + path, (req, res) -> {	
			if(pipeline!=null) {
			this.pipeline.addEvent(req.body());
			}
			if(listener!=null) {
				this.listener.notify(0, req.body());
			}
			return "ok";
		});

	}

	@Override
	public boolean addEvent(String event) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean addListener(ListenerInf listener) {
		this.listener = listener;
		return true;
	}

	@Override
	public void start() {
		// TODO Auto-generated method stub
		this.stream();
	}
	@Override
	public void stop() {
		// TODO Auto-generated method stub
		Spark.stop();
	}
}
