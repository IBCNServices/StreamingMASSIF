package idlab.massif.run;

import spark.Spark;

public class SparkIDTest {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		Spark.get("/test/:id", (request, response) ->request.params("id"));
	}

}
