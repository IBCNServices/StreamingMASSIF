# StreamingMASSIF

This is the implementation of the *StreamingMASSIF* platform, a streaming extension of the MASSIF platform.

StreamingMASSIF allows to perform cascading reasoning by combining various components. In its standard configuration it allows to filter meaningful events from a datastream through RDF Stream Processing, abstract the selection through DL reasoning and perform Complex Event Processing ontop of these abstraction.

Check the [wikipage](https://github.com/IBCNServices/StreamingMASSIF/wiki) for a more in depth explanation on how to use Streaming MASSIF!

How to cite [Streaming MASSIF](https://www.mdpi.com/1424-8220/18/11/3832):
```
@article{bonte2018streaming,
  title={Streaming MASSIF: Cascading Reasoning for Efficient Processing of IoT Data Streams},
  author={Bonte, Pieter and Tommasini, Riccardo and Della Valle, Emanuele and De Turck, Filip and Ongenae, Femke},
  journal={Sensors},
  volume={18},
  number={11},
  pages={3832},
  year={2018},
  publisher={Multidisciplinary Digital Publishing Institute}
}
```

How to cite [MASSIF](https://link.springer.com/article/10.1007/s10115-016-0969-1):
```
@article{bonte2017massif,
  title={The MASSIF platform: a modular and semantic platform for the development of flexible IoT services},
  author={Bonte, Pieter and Ongenae, Femke and De Backere, Femke and Schaballie, Jeroen and Arndt, D{\"o}rthe and Verstichel, Stijn and Mannens, Erik and Van de Walle, Rik and De Turck, Filip},
  journal={Knowledge and Information Systems},
  volume={51},
  number={1},
  pages={89--126},
  year={2017},
  publisher={Springer}
}
```

## Building and running MASSIF

### Requirements:

- Java 9+
- Maven2

### Build
To build the MASSIF app, call `mvn` and get the compiled `.jar`.
```shell
mvn clean compile assembly:single
mv target/massif-jar-with-dependencies.jar .
```

To build the MASSIF classes (e.g. for usage in higher level apps), call `mvn` to compile and install the project in the local repository, then add the package to the higher level app dependencies (see `pom.xml` snippet below):  
```shell
mvn install -Dmaven.test.skip=true 
```
```xml
<dependency>
    <groupId>be.ugent.idlab</groupId>
    <artifactId>massif</artifactId>
    <version>0.0.1</version>
</dependency>
```

### Run
To run MASSIF, call the compiled `.jar` from the command line as follows:
```shell
java -jar -Dlog4j.configurationFile=webfiles/log4j2.xml massif-jar-with-dependencies.jar
```
Calling this command will return something like this on the CLI:
```shell
21:27:32.313 [main] INFO  idlab.massif.run.Run - MASSIF STARTING
21:27:32.403 [main] INFO  idlab.massif.run.Run - MASSIF Listening on port 9000
21:27:32.403 [main] INFO  idlab.massif.run.Run - Access the MASSIF GUI on  localhost:9000 or register a configuration on localhost:9000/register
21:27:32.414 [main] INFO  idlab.massif.run.Run - MASSIF is ONLINE
```
Run options:
* `-p` : TCP port on which massif listens, default: `9000`

GUI:
* The MASSIF GUI is available on http://localhost:9000

### REST API
The MASSIF allow for direct management through GET/POST calls.
Here are the most important path:
* `<massif_url>/register`: register a configuration
* `<massif_url>/stop` : stop a certain query
* `<massif_url>/configs` : get all registered configs

### REST API call examples with the [cURL](https://linuxize.com/post/curl-post-request/) tool
List of active configs:
```
curl -X GET --verbose \
  --url "http://127.0.0.1:9000/configs"
```
* Note: returns `{}` if there are no active config.

Stop a config (pre-requisite:  config ID, see previous command):
```
curl -X POST --verbose \
  -H "Content-Type: application/json" \
  -d '19' \
  --url "http://127.0.0.1:9000/stop"
```
* Note: returns `ok`

Send a config:
```
curl -X POST --verbose \
  -H "Content-Type: application/json" \
  -d '{"configuration":{"0":[1],"1":[]},"components":{"0":{"type":"Source","impl":"kafkaSource","kafkaServer":"127.0.0.1:9092","kafkaTopic":"backblaze_smart"},"1":{"type":"Sink","impl":"httpGetSinkCombined","path":"1","config":""}}}' \
  --url "http://127.0.0.1:9000/register"
```
* Note: returns the config ID

Get a component running metrics (pre-requisite: component ID, see previous commands):
```
curl -X GET --verbose \
  --url "http://127.0.0.1:9000/monitor/1"
```


