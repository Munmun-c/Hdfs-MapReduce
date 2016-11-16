javac -classpath ./protobuf-java-3.0.0-beta-2.jar:./bin/Common/:./source/Common/:./source -d bin source/Common/*.java
javac -classpath ./protobuf-java-3.0.0-beta-2.jar:./bin/Common/:./source/Common/:./source -d bin source/INameNode/*.java
javac -classpath ./protobuf-java-3.0.0-beta-2.jar:./bin/Common/:./source/Common/:./source -d bin source/IDataNode/*.java
javac -classpath ./protobuf-java-3.0.0-beta-2.jar:./bin/Common/:./source/Common/:./source -d bin source/mapReduce/*.java

cd bin
java -classpath ../protobuf-java-3.0.0-beta-2.jar:. mapReduce.JobTracker
