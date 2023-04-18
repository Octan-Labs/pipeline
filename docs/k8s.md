bin/spark-submit \
--master k8s://F555453741847D0C5223F7254EDF95AD.gr7.ap-southeast-1.eks.amazonaws.com \
--deploy-mode cluster \
--name 'Hello world' \
--conf spark.eventLog.dir=s3a://datateam-spark/logs \
--conf spark.eventLog.enabled=true \
--conf spark.history.fs.inProgressOptimization.enabled=true \
--conf spark.history.fs.update.interval=5s \
--conf spark.kubernetes.namespace='spark-jobs' \
--conf spark.kubernetes.container.image=171092530978.dkr.ecr.ap-southeast-1.amazonaws.com/octan/sparkonk8s:0.0.5 \
--conf spark.kubernetes.container.image.pullPolicy=IfNotPresent \
--conf spark.kubernetes.driver.podTemplateFile='../k8s/driver_pod_template.yml' \
--conf spark.kubernetes.executor.podTemplateFile='../k8s/executor_pod_template.yml' \
--conf spark.kubernetes.authenticate.driver.serviceAccountName=spark \
--conf spark.dynamicAllocation.enabled=true \
--conf spark.dynamicAllocation.shuffleTracking.enabled=true \
--conf spark.dynamicAllocation.maxExecutors=100 \
--conf spark.dynamicAllocation.executorAllocationRatio=0.33 \
--conf spark.dynamicAllocation.sustainedSchedulerBacklogTimeout=30 \
--conf spark.dynamicAllocation.executorIdleTimeout=60s \
--conf spark.driver.memory=8g \
--conf spark.kubernetes.driver.request.cores=2 \
--conf spark.kubernetes.driver.limit.cores=4 \
--conf spark.executor.memory=8g \
--conf spark.kubernetes.executor.request.cores=2 \
--conf spark.kubernetes.executor.limit.cores=4 \
--conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
--conf spark.hadoop.fs.s3a.connection.ssl.enabled=false \
--conf spark.hadoop.fs.s3a.fast.upload=true \
s3a://datateam-spark/script.py \
s3a://datateam-spark/output

./bin/docker-image-tool.sh -t 0.0.5 -p ./kubernetes/dockerfiles/spark/bindings/python/Dockerfile build 

docker tag spark-py 171092530978.dkr.ecr.ap-southeast-1.amazonaws.com/octan/sparkonk8s:0.0.5
docker push 171092530978.dkr.ecr.ap-southeast-1.amazonaws.com/octan/sparkonk8s:0.0.5

docker run ghcr.io/exacaster/lighter:0.0.45-spark3.3.2 \
  -p 8080:8080 \
  -p 25333:25333 \
  -e LIGHTER_KUBERNETES_ENABLED=true \
  -e LIGHTER_KUBERNETES_MASTER=k8s://F555453741847D0C5223F7254EDF95AD.gr7.ap-southeast-1.eks.amazonaws.com
j