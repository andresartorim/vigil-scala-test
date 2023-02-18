# Vigil Scala Test

This is Vigil Scala test made by André Miranda

Used in this project
 - Scala 2.12.15
 - Spark 3.3.1
 - Scalatest Funsuite 3.2.11
 - SBT
 - JAVA 11
---
## Instructions

### Run tests
`sbt test`

### Build
`sbt publishLocal`

---

## Run VirgialScalaApp

#### Build jar locally
> sbt publishLocal

after that, you need to get the output path from your machine to get the jar path

replace `{{MACHINE-FOLDER}}` with the jar path on your machine

```sh
spark-submit --class org.vigil.devtest.VigilScalaApp --master local[*] /{{MACHINE-FOLDER}}/org.vigil/vgl-scala-test_2.12/0.1.0-SNAPSHOT/jars/vgl-scala-test_2.12.jar S3-INPUT-PATH S3-OUTPUT-PATH AWS-CREDENTIALS
```

using an input an output locally should be like this:

```sh
spark-submit --class org.vigil.devtest.VigilScalaApp --master local[*] /{{MACHINE-FOLDER}}/org.vigil/vgl-scala-test_2.12/0.1.0-SNAPSHOT/jars/vgl-scala-test_2.12.jar src/main/resources src/main/resources/output default
```
The output should be in `src/main/resources/output`

---

## Run VirgialScalaApp - BONUS

after that, you need to get the output path from your machine to get the jar path

```sh
spark-submit --class org.vigil.bonus.VigilScalaBonusApp --master local[*] /{{MACHINE-FOLDER}}/org.vigil/vgl-scala-test_2.12/0.1.0-SNAPSHOT/jars/vgl-scala-test_2.12.jar S3-INPUT-PATH S3-OUTPUT-PATH AWS-CREDENTIALS
```

using an input an output locally should be like this:

```sh
spark-submit --class org.vigil.bonus.VigilScalaBonusApp --master local[*] /{{MACHINE-FOLDER}}/org.vigil/vgl-scala-test_2.12/0.1.0-SNAPSHOT/jars/vgl-scala-test_2.12.jar src/main/resources src/main/resources/output/bonus default
```

The output should be in `src/main/resources/output/bonus`

---

### Running on IntelliJ

You can also run the tests through intelliJ using the correct project dependencies as mentioned in the beggining of this readme

Necessary:
- Configure arguments in program arguments
- Install Scala on IntelliJ
- Install SBT
