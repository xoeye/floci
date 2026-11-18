package io.floci.aws.services.ecs.model;

import io.quarkus.runtime.annotations.RegisterForReflection;

/**
 * The ENI ECS created for one awsvpc task, persisted so a restarted Floci, which knows no task,
 * deletes exactly these and never an interface someone else created in the subnet.
 */
@RegisterForReflection
public record EcsTaskNetworkInterface(String region, String networkInterfaceId) {
}
