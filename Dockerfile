FROM confluentinc/cp-kafka-connect-base:7.4.0

# Create the plugin directory
RUN mkdir -p /usr/share/java/plugins

# Copy the JAR file to the plugin directory
COPY target/custom-s3-sink-connector-0.0.1-SNAPSHOT.jar /usr/share/java/plugins/

# Install required plugins
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3:latest

# Set the plugin path environment variable
ENV CONNECT_PLUGIN_PATH="/usr/share/java/plugins,/usr/share/confluent-hub-components"
