# Use Flink as the base image
FROM confluentinc/cp-flink:1.20.0-cp1-java17

# Copy the jar from the target folder to the image
# The jar name will be: flinkalert-1.0-SNAPSHOT.jar (based on pom.xml artifactId and version)
COPY ./target/flinkalert-1.0-SNAPSHOT.jar /opt/flink/lib/