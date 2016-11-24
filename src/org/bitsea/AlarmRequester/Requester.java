package org.bitsea.AlarmRequester;

import org.apache.camel.main.Main;
import org.bitsea.AlarmRequester.rest.GeneralQueries;


public class Requester {
	
	private Main main;

	public static void main(String[] args) throws Exception {
		Requester arq = new Requester();
		arq.boot();
	}

	public void boot() throws Exception {
		main = new Main();
//		main.enableHangupSupport();
		main.bind("PatientInformation", new PatientInformation());
		main.bind("StationInformation", new StationInformation());
		main.bind("AverageQueries", new AverageQueries());
		main.addRouteBuilder(new GeneralQueries());
		main.run();
	}
}
