#FileStore CLI

Using Kafka as a filesystem ledger keeper, and an shared unique file storage medium like S3, Minio, NFS.

##Usage

###Export

_What it does....._

example: 
`./filestoreCLI -o export -t "kafka-topic-name" -p "/path/to/root" -k "kafka-0:9092" -g "project_unique_name" -d "Minio" -u "http://10.0.0.47:9000" -b "bucketName" -a minioadmin -s minioadmin`