package io.floci.aws.services.ecs.container;

import com.fasterxml.jackson.core.type.TypeReference;
import io.floci.aws.core.storage.AccountAwareStorageBackend;
import io.floci.aws.core.storage.StorageFactory;
import io.floci.aws.services.ecs.model.EcsTaskNetworkInterface;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;

import java.util.List;
import java.util.Map;

/**
 * The ENI each awsvpc task holds, keyed by task ARN. Task state is memory-only while EC2 persists
 * its interfaces, so without this record a restart leaves every running task's ENI holding a subnet
 * address that nothing will ever release.
 */
@ApplicationScoped
public class EcsTaskNetworkLedger {

    private final AccountAwareStorageBackend<EcsTaskNetworkInterface> entries;

    @Inject
    public EcsTaskNetworkLedger(StorageFactory storageFactory) {
        this.entries = storageFactory.create("ecs", "ecs-task-network-interfaces.json",
                new TypeReference<Map<String, EcsTaskNetworkInterface>>() {});
    }

    EcsTaskNetworkLedger(AccountAwareStorageBackend<EcsTaskNetworkInterface> entries) {
        this.entries = entries;
    }

    void record(String taskArn, String region, String networkInterfaceId) {
        entries.put(taskArn, new EcsTaskNetworkInterface(region, networkInterfaceId));
    }

    void forget(String taskArn) {
        entries.delete(taskArn);
    }

    void forget(String accountId, String taskArn) {
        entries.deleteForAccount(accountId, taskArn);
    }

    List<AccountAwareStorageBackend.AccountEntry<EcsTaskNetworkInterface>> allAccounts() {
        return entries.scanAllAccountEntries(key -> true);
    }
}
