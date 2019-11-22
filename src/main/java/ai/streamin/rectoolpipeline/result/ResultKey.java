package ai.streamin.rectoolpipeline.result;

import org.apache.beam.sdk.coders.AvroCoder;
import org.apache.beam.sdk.coders.DefaultCoder;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
@DefaultCoder(AvroCoder.class)
public class ResultKey {

	private String tradeId;
	private String book;
	private String pair;
}
