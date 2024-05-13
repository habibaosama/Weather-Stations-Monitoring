- `docker build -t weather-station-image:latest .`
- running the Maven command inside the Docker container interactively. Here's how you can do it:
 `docker run -it weather-station bash`
- then inside the container
- 
  `mvn exec:java -Dexec.mainClass="Main" -Dexec.args="Meteo 2 53 14"`


