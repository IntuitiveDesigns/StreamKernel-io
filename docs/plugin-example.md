# Plugin Example: SPI Moat

StreamKernel's moat is the runtime contract: plugin authors get a small Apache 2.0 SDK surface, while the source-available runtime owns lifecycle, batching, backpressure, policy flow, metrics, provenance, and DLQ behavior.

This example shows a transformer plugin. The same pattern applies to sources, sinks, cache providers, security plugins, DLQ serializers, and metrics providers.

## Transformer Implementation

```java
package example.plugins;

import com.intuitivedesigns.streamkernel.core.PipelinePayload;
import com.intuitivedesigns.streamkernel.core.Transformer;

public final class CustomerTierTransformer implements Transformer<String, String> {
    @Override
    public PipelinePayload<String> transform(PipelinePayload<String> input) {
        String tier = input.metadata().getOrDefault("customerTier", "standard");
        return input
                .withData(input.data())
                .withHeader("routingTier", tier);
    }
}
```

## Plugin Factory

```java
package example.plugins;

import com.intuitivedesigns.streamkernel.config.PipelineConfig;
import com.intuitivedesigns.streamkernel.core.Transformer;
import com.intuitivedesigns.streamkernel.metrics.MetricsRuntime;
import com.intuitivedesigns.streamkernel.spi.TransformerPlugin;

public final class CustomerTierTransformerPlugin implements TransformerPlugin {
    @Override
    public String id() {
        return "CUSTOMER_TIER";
    }

    @Override
    public Transformer<?, ?> create(PipelineConfig config, MetricsRuntime metrics) {
        return new CustomerTierTransformer();
    }
}
```

## Service Loader Registration

Create:

```text
src/main/resources/META-INF/services/com.intuitivedesigns.streamkernel.spi.TransformerPlugin
```

With:

```text
example.plugins.CustomerTierTransformerPlugin
```

## Pipeline Usage

```properties
transform.type=CUSTOMER_TIER
```

Depending on the profile, this can be used as a single transform or in a configured transform chain.

## Ownership Statement

The plugin above can live in a separate repository and remain under the author's chosen license. Implementing StreamKernel's Apache 2.0 SPI does not transfer plugin ownership to StreamKernel.
