package idlab.examples.generators;

import spark.Spark;

public class HTTPPostSinkTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		String path = "test";
		Spark.port(9090);
		Spark.post("/" + path, (req, res) -> {	
			System.out.println(req.body());
			
			return "ok";
		});
	}

}
