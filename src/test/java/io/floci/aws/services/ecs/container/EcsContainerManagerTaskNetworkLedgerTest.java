package io.floci.aws.services.ecs.container;

import io.floci.aws.config.EmulatorConfig;
import io.floci.aws.core.common.AwsException;
import io.floci.aws.core.common.RegionResolver;
import io.floci.aws.core.common.docker.ContainerBuilder;
import io.floci.aws.core.common.docker.ContainerDetector;
import io.floci.aws.core.common.docker.ContainerLifecycleManager;
import io.floci.aws.core.common.docker.ContainerLogStreamer;
import io.floci.aws.core.common.docker.LaunchedContainerAwsEnv;
import io.floci.aws.core.storage.AccountAwareStorageBackend;
import io.floci.aws.services.ec2.Ec2Service;
import io.floci.aws.services.ec2.model.NetworkInterface;
import io.floci.aws.services.ecr.registry.EcrRegistryManager;
import io.floci.aws.services.ecs.model.EcsTask;
import io.floci.aws.services.s3.S3Service;
import io.floci.aws.services.secretsmanager.SecretsManagerService;
import io.floci.aws.services.ssm.SsmService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import static io.floci.aws.services.ecs.container.AwsvpcTaskFixtures.awsvpcTaskWithoutEni;
import static io.floci.aws.services.ecs.container.AwsvpcTaskFixtures.taskDef;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.RETURNS_DEEP_STUBS;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * The task ENI record a restarted Floci, which knows no task, deletes the ENIs of its previous
 * run's tasks by. Two managers over one ledger stand for a run and the run after a restart.
 */
class EcsContainerManagerTaskNetworkLedgerTest {

    private static final String REGION = "us-east-1";

    private Ec2Service ec2Service;
    private EcsTaskNetworkLedger ledger;

    @BeforeEach
    void setUp() {
        ec2Service = mock(Ec2Service.class);
        NetworkInterface eni = new NetworkInterface();
        eni.setNetworkInterfaceId("eni-1");
        eni.setPrivateIpAddress("10.0.0.10");
        when(ec2Service.createNetworkInterface(any(), eq("subnet-1"), any(), any(), any(), any(), any()))
                .thenReturn(eni);
        ledger = new EcsTaskNetworkLedger(AccountAwareStorageBackend.inMemory("000000000000"));
    }

    private EcsContainerManager newManager() {
        EcsContainerManager manager = new EcsContainerManager(
                mock(ContainerBuilder.class), mock(ContainerLifecycleManager.class),
                mock(ContainerLogStreamer.class), mock(ContainerDetector.class),
                mock(EmulatorConfig.class, RETURNS_DEEP_STUBS), mock(RegionResolver.class),
                mock(LaunchedContainerAwsEnv.class), mock(SsmService.class), mock(SecretsManagerService.class),
                mock(S3Service.class), mock(EcrRegistryManager.class), mock(HostVolumePolicy.class),
                ec2Service, null, null, null, null);
        manager.taskNetworkLedger = ledger;
        return manager;
    }

    @Test
    void startupDeletesTheEniOfATaskThePreviousRunNeverStopped() {
        newManager().attachTaskNetwork(awsvpcTaskWithoutEni(), taskDef(null), REGION);

        newManager().releaseRecordedTaskNetworks();

        InOrder order = inOrder(ec2Service);
        order.verify(ec2Service).releaseNetworkInterfaceFromService(REGION, "eni-1");
        order.verify(ec2Service).deleteNetworkInterface(REGION, "eni-1");
    }

    @Test
    void aTaskThatStoppedLeavesNothingForStartupToDelete() {
        EcsContainerManager run = newManager();
        EcsTask task = awsvpcTaskWithoutEni();
        run.attachTaskNetwork(task, taskDef(null), REGION);
        run.releaseTaskNetwork(task, REGION);

        newManager().releaseRecordedTaskNetworks();

        verify(ec2Service, times(1)).deleteNetworkInterface(REGION, "eni-1");
    }

    @Test
    void startupTriesEachRecordedEniOnceEvenWhenEc2NoLongerHasIt() {
        newManager().attachTaskNetwork(awsvpcTaskWithoutEni(), taskDef(null), REGION);
        doThrow(new AwsException("InvalidNetworkInterfaceID.NotFound", "gone", 400))
                .when(ec2Service).deleteNetworkInterface(REGION, "eni-1");

        EcsContainerManager restarted = newManager();
        restarted.releaseRecordedTaskNetworks();
        restarted.releaseRecordedTaskNetworks();

        verify(ec2Service, times(1)).deleteNetworkInterface(REGION, "eni-1");
    }
}
