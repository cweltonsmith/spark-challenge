# spark-challenge

## Prerequisites

- VS Code
- VS Code Extension: Maven for Java
- VS Code Extension: Java Extension Pack

## Instructions 1 - Start New Maven Project

1. Create a new project using Maven (you do not need to fork this repo).
1. There should be a folder on your laptop where you keep your git projects. Go to this folder, e.g. 44517.
1. Open this parent folder in VS Code.
1. Click the VS Code Extensions icon. Verify you have the two required extensions. If not, install them.
1. Click the VS Code Explorer icon. From the menu, select:
1. View / Command Palette / Maven: Create Maven Project / archetype-quickstart-jdk8 / most recent version.
1. When the folder window opens, click your parent folder up at the top, click "Select Destination Folder".

## Instructions 2 - Interactive Mode

```Bash
groupId: edu.nwmissouri.yourname
artifactId: spark-challenge
version: HIT ENTER
package: HIT ENTER
Y: HIT ENTER
```

You will now have a spark-challenge project folder. Exit VS Code.

## Instructions 3 - Code the project

Change directory into your new spark-challenge folder. Right-click and open your spark-challenge folder in VS Code.

Add a basic README.md. Use it to store your notes and commands.

When VS Code asks: "A build file was modified. Do you want to synchronize the Java classpath/configuration?" Answer "Always" to allow VS Code to generate these artifacts automatically.

## Instructions 4 - Add to POM.xml

Copy the POM.xml from this repo to yours. Use CTRL-F to search for "isl" for "Intelligent Systems Lab".
Change each occurance to match yourname in your groupId instead.

## Prepare the Code

```PowerShell
mvn clean
mvn compile
mvn assembly:single
```

## Execute

```Bash
java -cp target/spark-challenge-1.0.0-jar-with-dependencies.jar edu.nwmissouri.isl.App "data.txt"
```

## Challenges - Getting Started

1. Where does program execution begin?
1. Update the main method to check that exactly one argument is provided.
1. If not, output a message and call System.exit(0);
1. If so, call a new method named process that uses the first item in the arg array.
1. Add the following imports to App.java.

```Java
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import scala.Tuple2;
import java.util.Arrays;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.util.Comparator;
```

## Challenges - create a process method

Create a new private static void process method that takes one argment, a String containting the fileName (provided in the args).

```Java

    // define a spark configuration
    // setMaster to local and setAppName to Challenge
    SparkConf sparkConf = new SparkConf()...

    // define a new JavaSparkContext, pass in the spark configuration object
    JavaSparkContext sparkContext = ...

    // use sparkContext.textFile() to read data into RDD given the fileName provided
    JavaRDD<String> inputFile = sparkContext.textFile(fileName);

    // use Java API to flatMap text into an RDD of strings (words)
    // In 2.0, FlatMapFunction.call() returns an Iterator rather than Iterable.
    // replace what must this be with the correct variable name
    JavaRDD<String> wordsFromFile = inputFile.flatMap( whatMustThisBe -> Arrays.asList(line.split(" ")).iterator());

    // use wordsFromFile and mapToPair to create a new Tuple2 for each word
    // the tuple is in the form (word, 1)
    // Replace each T below to create a JavaPairRDD with the associated types
    JavaPairRDD<T, T> countData = wordsFromFile.mapToPair(word -> new Tuple2(word, 1))
        .reduceByKey((x, y) -> (int) x + (int) y);

    // create new JavaPairRDD that reverses the tuple to (n, word)
    // use countData.mapToPair and provide a simple function that
    // takes each pair p and outputs a new Tuple2 with p._2 first, then p._1.
    // then call .sortByKey() to sort in reverse order,
    // pass in Comparator.reverseOrder() to the sortByKey method
    JavaPairRDD<Integer, String> output = ...

    // save results to a folder (RDDs are complex) - provide a simple string value.
    String outputFolder = ...

    // get the path to your outputFolder
    Path path = FileSystems.getDefault().getPath(outputFolder);

    // use FileUtils to delete them quietly
    // It does not work.
    // google and find the import statement required. Add it to this file.
    FileUtils.deleteQuietly(path.toFile());

    // use call saveAsTextFile on your output JavaPairRDD to save your results.
    // Pass in the variable holding your simple string
    add code here

    // close your spark context
    add code here
```

## Challenges - Creating data

1. What must your data file be named to make the execution command work?
1. Create this file. Use data from one of these links.
1. Use Maven to clean compile and build your fat jar file.
1. Use java to run your App by including the fat jar in your classpath.

java -cp target/spark-challenge-1.0.0-jar-with-dependencies.jar edu.nwmissouri.chasesmith.App "input.txt"
