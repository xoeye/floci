package io.github.hectorvent.floci.services.lambda;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.quarkus.runtime.StartupEvent;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.enterprise.event.Observes;
import jakarta.enterprise.inject.Instance;

/** Fills persisted functions' layer volumes at startup; {@code Instance<>} avoids loading LambdaService when off. */
@ApplicationScoped
public class LayerVolumePrefillStartup {

    void onStart(@Observes StartupEvent event, EmulatorConfig config, Instance<LambdaService> lambdaService) {
        EmulatorConfig.LambdaServiceConfig lambda = config.services().lambda();
        if (lambda.enabled() && lambda.layerVolumesEnabled() && lambda.layerVolumesPrefill()) {
            lambdaService.get().prefillLayerVolumes();
        }
    }
}
