mvn compile exec:java -Dexec.mainClass=ai.streamin.rectoolpipeline.RecToolPipeline  -Dexec.args="--runner=DataflowRunner --project=streamin-259422 --stagingLocation=gs://streamin-dataflow/staging --templateLocation=gs://streamin-dataflow/templates/RECTOOL"

gcloud dataflow jobs run rectool --gcs-location gs://streamin-dataflow/templates/RECTOOL --parameters referenceFile=gs://streamin-dataflow/rec/REC_VAN_REF.txt,candidateFile=gs://streamin-dataflow/rec/REC_VAN_CAN.txt

RECTOOL_metadata is stored in same directory as template

gsutil cp rec/REC_VAN_REF.txt gs://streamin-data-flow/rec/REC_VAN_REF.txt

gsutil cp RECTOOL_metadata  gs://streamin-data-flow/templates/RECTOOL_metadata
