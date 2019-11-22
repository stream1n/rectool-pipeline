package ai.streamin.rectoolpipeline.result;

import java.io.Serializable;

import lombok.Data;

@Data
public class Result implements Serializable {/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private String tradeId = "";
	private String book = "";
	private String pair = "";
	private double pv = 0.;
	private boolean isMissing = true;
	
}
