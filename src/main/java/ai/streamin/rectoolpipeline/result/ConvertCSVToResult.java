package ai.streamin.rectoolpipeline.result;

import java.io.IOException;
import java.io.StringReader;

import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.values.KV;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;

public class ConvertCSVToResult extends DoFn<String, KV<ResultKey, Result>> {

	private static final long serialVersionUID = 1L;

	private static final String[] FILE_HEADER_MAPPING = {
			"TradeId","Book","Pair","PV"
	};

	private String delimiter;

	public ConvertCSVToResult(String delimiter) {
		this.delimiter = delimiter;
	}

	@ProcessElement
	public void processElement(ProcessContext ctx) throws IllegalArgumentException, IOException {

		try(CSVParser parser = new CSVParser(new StringReader(ctx.element()), CSVFormat.DEFAULT
				.withDelimiter(delimiter.charAt(0))
				.withHeader(FILE_HEADER_MAPPING))) {

			CSVRecord csvrecord = parser.getRecords().get(0);

			if (csvrecord.get("PV").contains("PV") ){
				return;
			}

			Result record = new Result();
			record.setTradeId(csvrecord.get("TradeId"));
			record.setBook(csvrecord.get("Book"));
			record.setPair(csvrecord.get("Pair"));
			record.setPv(Double.valueOf(csvrecord.get("PV")));
			record.setMissing(false);

			ctx.output(KV.of(new ResultKey(record.getTradeId(), record.getBook(), record.getPair()), record));
		}
	}

}