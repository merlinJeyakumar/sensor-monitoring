FROM amazoncorretto:21.0.10
WORKDIR /app
COPY build/install/sensor-monitoring/ /app/
RUN chmod +x /app/bin/sensor-monitoring
ENV JAVA_TOOL_OPTIONS="-Xms32m -Xmx128m -XX:ActiveProcessorCount=2 -XX:+ExitOnOutOfMemoryError"
USER 10001:10001
EXPOSE 8080 8081 3344/udp 3355/udp
ENTRYPOINT ["/app/bin/sensor-monitoring"]
