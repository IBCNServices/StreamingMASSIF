# StreamingMASSIF

This is the implementation of the StreamingMASSIF platform, a streaming extension of the MASSIF platform.

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

## Building and running MASSIF:
build:
`mvn clean compile assembly:single`
run:
`java -jar massif-0.0.1-jar-with-dependencies.jar`


