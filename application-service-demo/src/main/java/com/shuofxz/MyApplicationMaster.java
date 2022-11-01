package com.shuofxz;

import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.net.NetUtils;
import org.apache.hadoop.yarn.api.protocolrecords.RegisterApplicationMasterResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.AMRMClient.ContainerRequest;
import org.apache.hadoop.yarn.client.api.async.AMRMClientAsync;

import org.apache.commons.logging.Log;
import org.apache.hadoop.yarn.client.api.async.NMClientAsync;
import org.apache.hadoop.yarn.client.api.async.impl.NMClientAsyncImpl;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class MyApplicationMaster {

    private static final Log log = LogFactory.getLog(MyApplicationMaster.class);

    @SuppressWarnings("rawtypes")
    AMRMClientAsync amRMClient = null;
    NMClientAsyncImpl amNMClient = null;

    AtomicInteger numTotalContainers = new AtomicInteger(10);
    AtomicInteger numCompletedContainers = new AtomicInteger(0);
    ExecutorService exeService = Executors.newCachedThreadPool();
    Map<ContainerId, Container> runningContainers = new ConcurrentHashMap<ContainerId, Container>();

    private final AtomicInteger sleepSeconds = new AtomicInteger(0);


    public static void main(String[] args) throws Exception {
        MyApplicationMaster am = new MyApplicationMaster();
        am.run();
        am.waitComplete();
    }


    @SuppressWarnings("unchecked")
    void run() throws YarnException, IOException {

        // logInformation();
        Configuration conf = new Configuration();

        // 1 create amRMClient
        amRMClient = AMRMClientAsync.createAMRMClientAsync(1000, new RMCallbackHandler());
        amRMClient.init(conf);
        amRMClient.start();

        // 2 Create nmClientAsync
        amNMClient = new NMClientAsyncImpl(new NMCallbackHandler());
        amNMClient.init(conf);
        amNMClient.start();

        // 3 register with RM and this will heart beating to RM
        RegisterApplicationMasterResponse response = amRMClient
                .registerApplicationMaster(NetUtils.getHostname(), -1, "");

        // 4 Request containers
        response.getContainersFromPreviousAttempts();
        // 4.1 check resource
        long maxMem = response.getMaximumResourceCapability().getMemorySize();
        int maxVCores = response.getMaximumResourceCapability().getVirtualCores();

        // 4.2 request containers base on avail resource
        for (int i = 0; i < numTotalContainers.get(); i++) {
            ContainerRequest containerAsk = new ContainerRequest(
                    //100*10M + 1vcpu
                    Resource.newInstance(100, 1), null, null,
                    Priority.newInstance(0));
            amRMClient.addContainerRequest(containerAsk);
        }
    }

    void waitComplete() throws YarnException, IOException{
        while(numTotalContainers.get() != numCompletedContainers.get()){
            try{
                Thread.sleep(1000);
                log.info("waitComplete" +
                        ", numTotalContainers=" + numTotalContainers.get() +
                        ", numCompletedConatiners=" + numCompletedContainers.get());
            } catch (InterruptedException ex){}
        }
        log.info("ShutDown exeService Start");
        exeService.shutdown();
        log.info("ShutDown exeService Complete");
        amNMClient.stop();
        log.info("amNMClient stop Complete");
        amRMClient.unregisterApplicationMaster(FinalApplicationStatus.SUCCEEDED, "dummy Message", null);
        log.info("unregisterApplicationMaster Complete");
        amRMClient.stop();
        log.info("amRMClient stop Complete");
    }

    // 之前版本（如 2.6）使用的时候还是实现的接口，不知为何后面改为了继承抽象类
    // 增加了方法 onContainersUpdated，猜测是支持 container 容量变化使用的
    private class RMCallbackHandler extends AMRMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainersCompleted(List<ContainerStatus> statuses) {
            for (ContainerStatus status : statuses) {
                log.info("Container completed: " + status.getContainerId().toString()
                        + " exitStatus=" + status.getExitStatus());
                if (status.getExitStatus() != 0) {
                    log.error("Container return error status: " + status.getExitStatus());
                    log.warn("Need rerun container!");
                    // do something restart container
                    continue;
                }
                ContainerId containerId = status.getContainerId();
                runningContainers.remove(containerId);
                numCompletedContainers.addAndGet(1);
            }
        }

        @Override
        public void onContainersAllocated(List<Container> containers) {
            for (Container c : containers) {
                log.info("Container Allocated, id = " + c.getId() + ", containerNode = " + c.getNodeId());
                exeService.submit(new LaunchContainerTask(c));
            }

        }

        @Override
        public void onContainersUpdated(List<UpdatedContainer> containers) {

        }

        @Override
        public void onShutdownRequest() {

        }

        @Override
        public void onNodesUpdated(List<NodeReport> updatedNodes) {

        }

        @Override
        public float getProgress() {
            return 0;
        }

        @Override
        public void onError(Throwable e) {
            amRMClient.stop();
        }
    }

    // 同理 nm 的 CallbackHandler 也推荐使用抽象类，多了方法 onContainerResourceIncreased，支持扩容容器
    // 也有问题是，为什么不是接口继承而是抽象类继承？猜测可能是为了避免接口使用继承
    private class NMCallbackHandler extends NMClientAsync.AbstractCallbackHandler {
        @Override
        public void onContainerStarted(ContainerId containerId, Map<String, ByteBuffer> allServiceResponse) {
            log.info("Container Stared " + containerId.toString());
        }

        @Override
        public void onContainerStatusReceived(ContainerId containerId, ContainerStatus containerStatus) {

        }

        @Override
        public void onContainerStopped(ContainerId containerId) {

        }

        @Override
        public void onStartContainerError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onContainerResourceIncreased(ContainerId containerId, Resource resource) {

        }

        @Override
        public void onGetContainerStatusError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onIncreaseContainerResourceError(ContainerId containerId, Throwable t) {

        }

        @Override
        public void onStopContainerError(ContainerId containerId, Throwable t) {

        }
    }

    private class LaunchContainerTask implements Runnable {
        Container container;

        public LaunchContainerTask(Container container) {
            this.container = container;
        }

        @Override
        public void run() {
            LinkedList<String> commands = new LinkedList<>();
            commands.add("sleep " + sleepSeconds.addAndGet(1));
            ContainerLaunchContext ctx = ContainerLaunchContext.newInstance(null, null, commands, null, null, null);
            // 这里去执行 amNMClient 的回调
            amNMClient.startContainerAsync(container, ctx);
        }
    }
}
