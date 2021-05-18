FROM openjdk:11-jdk-slim
ARG JAR_FILE=target/mapping-0.0.1-SNAPSHOT.jar
COPY ${JAR_FILE} app.jar
COPY ./limes.jar limes.jar
COPY ./example_data.ttl example_data.ttl
COPY ./example_limes_config.xml example_limes_config.xml
ENTRYPOINT ["java","-jar","/app.jar"]
