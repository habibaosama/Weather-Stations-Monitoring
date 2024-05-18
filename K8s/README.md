for elastic search and kibana
pull image from this link
https://hub.docker.com/r/nshou/elasticsearch-kibana 
`docker pull nshou/elasticsearch-kibana`
then deploy the image using this yamal file 
`kubectl apply -f elastic-kibana-deployment.yaml`
may need these command 

`kubectl get  services`
`kubectl port-forward service/kibana-load-balancer 5601:5601`
`kubectl get svc`