package ai.streamin.rectoolpipeline;

import java.io.IOException;
import java.time.Instant;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.datastore.DatastoreIO;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;

import ai.streamin.rectoolpipeline.options.Options;
import ai.streamin.rectoolpipeline.result.ConvertCSVToResult;
import ai.streamin.rectoolpipeline.result.ReconcileResultsFn;
import ai.streamin.rectoolpipeline.result.Result;
import ai.streamin.rectoolpipeline.result.ResultKey;

public class RecToolPipeline {

	public static void main(String[] args) throws IOException {
		Options options = PipelineOptionsFactory.fromArgs(args).withValidation().as(Options.class);
		run(options);
	}
	
	private static PipelineResult run(Options options) throws IOException {

		Pipeline pipeline = Pipeline.create(options);

		String project = options.as(GcpOptions.class).getProject();
		
		long recTime = Instant.now().toEpochMilli();
		
		PCollection<KV<ResultKey, Result>> candidate = pipeline
				.apply("Read Candidate File", TextIO.read().from(options.getCandidateFile().get()))
				.apply("Convert Candidate To Records", ParDo.of(new ConvertCSVToResult(options.getCsvDelimiter().get())));
		
		PCollection<KV<ResultKey, Result>> reference = pipeline
				.apply("Read Reference File", TextIO.read().from(options.getReferenceFile().get()))
				.apply("Convert Reference To Records", ParDo.of(new ConvertCSVToResult(options.getCsvDelimiter().get())));
		
        TupleTag<Result> candidateTag = new TupleTag<>();
        TupleTag<Result> referenceTag = new TupleTag<>();
        
        KeyedPCollectionTuple
                .of(candidateTag, candidate)
                .and(referenceTag, reference)
                .apply(CoGroupByKey.create())
                .apply("Reconcile", ParDo.of(new ReconcileResultsFn(recTime, candidateTag, referenceTag)))
                .apply("Write To Datastore",DatastoreIO.v1().write().withProjectId(project));
        
		PipelineResult result = pipeline.run();

		try {
			result.waitUntilFinish();
		} catch (Exception exc) {
			result.cancel();
		}

		return result;
	}
}
