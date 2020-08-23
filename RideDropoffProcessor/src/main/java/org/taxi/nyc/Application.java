
package org.taxi.nyc;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

import java.text.SimpleDateFormat;
import java.time.Duration;
import java.util.Date;
import java.util.function.Function;


@SpringBootApplication
public class Application {

	public static final String ISO_8601_24H_FULL_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSSXXX";
	final SimpleDateFormat sdf = new SimpleDateFormat(ISO_8601_24H_FULL_FORMAT);

	private static final Logger logger = LoggerFactory.getLogger(Application.class);

	public static void main(String[] args) {
		SpringApplication.run(Application.class);
	}

	@Bean
	public Function<Flux<TaxiStatusUpdatePayload>, Flux<RideAveragePayload>> processDropoffRideAverages() {
		return flux -> flux.log().window(Duration.ofSeconds(20)).flatMap(this::calculateAverage);
	}

	private Mono<RideAveragePayload> calculateAverage(Flux<TaxiStatusUpdatePayload> flux) {
		// Aggregate the events in those windows
		return flux
				.reduce(new Accumulator(1, 1, 1),
						(a, taxiUpdate) -> new Accumulator(
								a.getRideCount() + 1,
								a.getTotalMeter() + taxiUpdate.getMeterReading(),
								a.getTotalPassengers() + taxiUpdate.getPassengerCount()))
				// Calculate the window average in RideAveragePayload objects'
				.map(accumulator -> new RideAveragePayload((accumulator.getTotalMeter() / accumulator.getRideCount()),
						20, ((double) accumulator.getTotalPassengers() / accumulator.getRideCount()),
						accumulator.getRideCount(), sdf.format(new Date())))
				.log();
	}


	static class Accumulator {

		private int rideCount;
		private double totalMeter;
		private int totalPassengers;

		public Accumulator(int rideCount, double totalMeter, int totalPassengers) {
			this.rideCount = rideCount;
			this.totalMeter = totalMeter;
			this.totalPassengers = totalPassengers;
		}

		public int getRideCount() {
			return rideCount;
		}

		public void setRideCount(int rideCount) {
			this.rideCount = rideCount;
		}

		public double getTotalMeter() {
			return totalMeter;
		}

		public void setTotalMeter(double totalMeter) {
			this.totalMeter = totalMeter;
		}

		public int getTotalPassengers() {
			return totalPassengers;
		}

		public void setTotalPassengers(int totalPassengers) {
			this.totalPassengers = totalPassengers;
		}
	}

}
