@echo off
set MAVEN_OPTS=-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=5005
mvn exec:java -Dexec.mainClass="com.nan.kafkasimulator.MainApplication" -Dexec.args="" -Dexec.classpathScope=compile
