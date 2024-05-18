- `docker build -t weather-station-image:latest .`
- running the Maven command inside the Docker container interactively. Here's how you can do it:
 `docker run -it weather-station-image:latest bash`
- then inside the container
- 
  `mvn exec:java -Dexec.mainClass="Main" -Dexec.args="Meteo 2 53 14"`

- if you want to test consumer command in windows
` .\bin\windows\kafka-console-consumer.bat --topic weather-station-topic --from-beginning --bootstrap-server localhost:9092`


