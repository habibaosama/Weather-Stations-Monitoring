FROM maven:3.9.2-eclipse-temurin-17-alpine

# Install libstdc++6
RUN apk add --no-cache libstdc++

WORKDIR /central-base

COPY src ./src
COPY pom.xml .

RUN mvn clean install

CMD ["mvn","exec:java"]
