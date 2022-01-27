package org.apache.beam.runners.samza;

import org.apache.beam.runners.core.construction.Environments;
import org.apache.beam.runners.portability.PortableRunner;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.options.PortablePipelineOptions;
import org.apache.beam.sdk.transforms.Create;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;


public class TestPortablePipeline {

  public static void main(String... args) {
    PortablePipelineOptions options = PipelineOptionsFactory.fromArgs(args).as(PortablePipelineOptions.class);
    options.setDefaultEnvironmentType(Environments.ENVIRONMENT_LOOPBACK);
    options.setJobEndpoint("localhost:8099");
    options.setRunner(PortableRunner.class);

    Pipeline p = Pipeline.create(options);


    p
        .apply(Create.of(1L, 2L, 3L))
        .apply(
            "First ParDo",
            ParDo.of(
                new DoFn<Long, Long>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    context.output(context.element());
                  }}))
        .apply(
            "Second ParDo",
            ParDo.of(new DoFn<Long, Long>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                context.output(context.element());
              }}));

    p.run().waitUntilFinish();

  }
}
