PATH:=${JAVA_HOME}/bin:${PATH}
HADOOP_PATH=/usr/local/hadoop
NEW_CLASSPATH=${HADOOP_PATH}/*:${CLASSPATH}:lib/json-simple-1.1.1.jar

SRC = $(wildcard *.java) 

all: build

build: ${SRC}
	${JAVA_HOME}/bin/javac -Xlint -classpath ${NEW_CLASSPATH} ${SRC}
	${JAVA_HOME}/bin/jar cvf build.jar *.class lib