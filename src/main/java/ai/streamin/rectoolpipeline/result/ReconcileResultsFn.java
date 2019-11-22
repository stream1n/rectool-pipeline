package ai.streamin.rectoolpipeline.result;

import static com.google.datastore.v1.client.DatastoreHelper.makeKey;
import static com.google.datastore.v1.client.DatastoreHelper.makeValue;

import java.io.IOException;

import java.util.UUID;

import org.apache.beam.sdk.transforms.DoFn;

import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.TupleTag;

import com.google.datastore.v1.Entity;

public class ReconcileResultsFn extends DoFn<KV<ResultKey,CoGbkResult>, Entity> {

	private static final long serialVersionUID = 1L;

	private long recTime;
	private TupleTag<Result> candidateTag;
	private TupleTag<Result> referenceTag;

	public ReconcileResultsFn(long recTime, TupleTag<Result> candidateTag, TupleTag<Result> referenceTag) {
		this.recTime = recTime;
		this.candidateTag = candidateTag;
		this.referenceTag = referenceTag;
	}

	@ProcessElement
	public void processElement(ProcessContext c) throws IllegalArgumentException, IOException {

		Result _candidate = new Result();
		Result _reference = new Result();

		for(Result can: c.element().getValue().getAll(candidateTag)) {
			_candidate = can;
		}

		for(Result ref : c.element().getValue().getAll(referenceTag)) {
			_reference = ref;
		}

		Entity entity = Entity
				.newBuilder()
				.setKey(makeKey("recResults", UUID.randomUUID().toString()))
				.putProperties("recTime", makeValue(recTime).build())
				.putProperties("tradeId", makeValue(c.element().getKey().getTradeId()).build())
				.putProperties("book", makeValue(c.element().getKey().getBook()).build())
				.putProperties("pair", makeValue(c.element().getKey().getPair()).build())
				.putProperties("isReferenceMissing", makeValue(_reference.isMissing()).build())
				.putProperties("isCandidateMissing", makeValue(_candidate.isMissing()).build())
				.putProperties("reference", makeValue(_reference.getPv()).build())
				.putProperties("candidate", makeValue(_candidate.getPv()).build())
				.putProperties("diff", makeValue(Math.abs(_reference.getPv() - _candidate.getPv())).build())
				.build();

		c.output(entity);		
	}
}