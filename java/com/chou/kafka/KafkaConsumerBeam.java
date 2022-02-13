package com.chou.kafka;

import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.kafka.KafkaIO;
import org.apache.beam.sdk.options.PipelineOptions;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.SimpleFunction;
import org.apache.beam.sdk.transforms.Values;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdks.java.io.kafka.repackaged.com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

public class KafkaConsumerBeam {
	static final String TOKENIZER_PATTERN = "[^\\p{L}]+";

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		PipelineOptions options = PipelineOptionsFactory.create();

		// Create the Pipeline object with the options we defined above.
		Pipeline p = Pipeline.create(options);

		p.apply(KafkaIO.<Long, String>read().withBootstrapServers("[::1]:9092").withTopic("quickstart-events")
				.withKeyDeserializer(LongDeserializer.class).withValueDeserializer(StringDeserializer.class)
				.updateConsumerProperties(ImmutableMap.of("auto.offset.reset", (Object) "earliest"))
				// We're writing to a file, which does not support unbounded
				// data sources. This line makes it bounded to
				// the first 5 records.
				// In reality, we would likely be writing to a data source that
				// supports unbounded data, such as BigQuery.
				.withMaxNumRecords(5)
				.withoutMetadata() // PCollection<KV<Long, String>>
		).apply(Values.<String>create()).apply("ExtractWords", ParDo.of(new DoFn<String, String>() {
			@ProcessElement
			public void processElement(ProcessContext c) {
				//System.out.print("here");
				for (String word : c.element().split(TOKENIZER_PATTERN)) {
					if (!word.isEmpty()) {
						c.output(word);
					}
				}
			}
		})).apply(Count.<String>perElement())
				.apply("FormatResults", MapElements.via(new SimpleFunction<KV<String, Long>, String>() {
					@Override
					public String apply(KV<String, Long> input) {
						System.out.println(input.getKey() + ": " + input.getValue());
						return input.getKey() + ": " + input.getValue();
					}
				})).apply(TextIO.write().to("wordcounts"));

		p.run().waitUntilFinish();
	}

}
