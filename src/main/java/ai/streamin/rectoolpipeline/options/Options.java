package ai.streamin.rectoolpipeline.options;

import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.ValueProvider;

public interface Options extends PipelineOptions {
	
	@Description("Reference File")
	ValueProvider<String> getReferenceFile();

	void setReferenceFile(ValueProvider<String> value);
	
	@Description("Candidate File")
	ValueProvider<String> getCandidateFile();

	void setCandidateFile(ValueProvider<String> value);
	
	@Description("Record Delimeter")
	ValueProvider<String> getCsvDelimiter();

	void setCsvDelimiter(ValueProvider<String> value);
	
}