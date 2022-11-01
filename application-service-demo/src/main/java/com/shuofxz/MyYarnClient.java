package com.shuofxz;

import org.apache.commons.io.FilenameUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.ipc.Client;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ClassUtil;
import org.apache.hadoop.yarn.api.ApplicationConstants;
import org.apache.hadoop.yarn.api.protocolrecords.GetNewApplicationResponse;
import org.apache.hadoop.yarn.api.records.*;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.client.api.YarnClientApplication;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hadoop.yarn.exceptions.YarnException;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.yarn.util.ConverterUtils;
import org.apache.log4j.Logger;

public class MyYarnClient {
    static private Logger log = Logger.getLogger("my-yarn-client");

    public static void main(String[] args) throws IOException, YarnException {
        YarnConfiguration conf = new YarnConfiguration();

        // 1 创建并启动YarnClient
        YarnClient yarnClient = YarnClient.createYarnClient();
        yarnClient.init(conf);
        /*
        YarnClient 内容通过 ApplicationClientProtocol 与 ResourceManager 通信，
        跟踪进去可以在 YarnClientImpl 找到 rpc，
        this.rmClient = (ApplicationClientProtocol)ClientRMProxy.createRMProxy(this.getConfig(), ApplicationClientProtocol.class);
         */
        yarnClient.start();


        // 2 通过YarnClient创建Application
        YarnClientApplication app = yarnClient.createApplication();
        // GetNewApplicationResponse 中包含了 ApplicationId, ResourceCapability 等内容
        GetNewApplicationResponse appResponse = app.getNewApplicationResponse();


        // 3 完善 ApplicationSubmissionContext 所需内容
        // 需要配置 appId、queue、resource、priority 等等
        ApplicationSubmissionContext appContext = app.getApplicationSubmissionContext();
        ApplicationId applicationId = appContext.getApplicationId();


        // 3.1 设置application name
        appContext.setApplicationName("my-test-app");
        // 3.2 设置ContainerLaunchContext
        // localResources, env, commands 等
        // application master 的 jar 放到 localResources 中
        ContainerLaunchContext amContainerCtx = createAMContainerLaunchContext(
                conf, app.getApplicationSubmissionContext().getApplicationId());
        appContext.setAMContainerSpec(amContainerCtx);
        // 3.3 设置优先级
        Priority pri = Priority.newInstance(0);
        appContext.setPriority(pri);
        // 3.4 设置队列
        appContext.setQueue("default");
        // 3.5 设置 am 资源
        int amMemory = 2048;
        int amVCores = 2;
        Resource capability = Resource.newInstance(amMemory, amVCores);
        appContext.setResource(capability);

        // 4 提交Application
        // 这里交给 YarnClientImpl 执行 rmClient.submitApplication(request)，通过 RPC ApplicationClientProtocol 提交到 RM
        ApplicationId appId = yarnClient.submitApplication(appContext);

        // 5 获取Application信息
        monitorApplicationReport(yarnClient, appId);
    }

    private static ContainerLaunchContext createAMContainerLaunchContext(
            Configuration conf, ApplicationId appId) throws IOException {
        // localResources 中存储需要的文件（app master jar、log4j properties 等）
        Map<String, LocalResource> localResources = new HashMap<String, LocalResource>();
        // todo app master jar 在这里配置

        FileSystem fs = FileSystem.get(conf);
        String thisJar = ClassUtil.findContainingJar(Client.class);
        String thisJarBaseName = FilenameUtils.getName(thisJar);
        log.info("thisJar is " + thisJar);

        // addToLocalResources(fs, thisJar, thisJarBaseName, appId.toString(), localResources);

        //Set CLASSPATH environment
        Map<String, String> env = new HashMap<String, String>();
        StringBuilder classPathEnv = new StringBuilder(
                ApplicationConstants.Environment.CLASSPATH.$$());
        classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
        classPathEnv.append("./*");
        for (String c : conf.getStrings(
                        YarnConfiguration.YARN_APPLICATION_CLASSPATH,
                        YarnConfiguration.DEFAULT_YARN_CROSS_PLATFORM_APPLICATION_CLASSPATH)) {
            classPathEnv.append(ApplicationConstants.CLASS_PATH_SEPARATOR);
            classPathEnv.append(c.trim());
        }

        if (conf.getBoolean(YarnConfiguration.IS_MINI_YARN_CLUSTER, false)) {
            classPathEnv.append(':');
            classPathEnv.append(System.getProperty("java.class.path"));
        }
        env.put(ApplicationConstants.Environment.CLASSPATH.name(), classPathEnv.toString());

        //Build the execute command
        List<String> commands = new LinkedList<String>();
        StringBuilder command = new StringBuilder();
        command.append(ApplicationConstants.Environment.JAVA_HOME.$$()).append("/bin/java  ");
        command.append("-Dlog4j.configuration=container-log4j.properties ");
        command.append("-Dyarn.app.container.log.dir=" +
                ApplicationConstants.LOG_DIR_EXPANSION_VAR + " ");
        command.append("-Dyarn.app.container.log.filesize=0 ");
        command.append("-Dhadoop.root.logger=INFO,CLA ");
        command.append("trumanz.yarnExample.ApplicationMaster ");
        command.append("1>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout ");
        command.append("2>" + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr ");
        commands.add(command.toString());

        ContainerLaunchContext amContainer = ContainerLaunchContext
                .newInstance(localResources, env, commands, null, null, null);

        // Setup security tokens 权限认证
        if (UserGroupInformation.isSecurityEnabled()) {
            Credentials credentials = new Credentials();
            String tokenRenewer = conf.get(YarnConfiguration.RM_PRINCIPAL);
            if (tokenRenewer == null || tokenRenewer.length() == 0) {
                throw new IOException(
                        "Can't get Master Kerberos principal for the RM to use as renewer");
            }

            // For now, only getting tokens for the default file-system.
            final Token<?> tokens[] = fs.addDelegationTokens(tokenRenewer, credentials);
            if (tokens != null) {
                for (Token<?> token : tokens) {
                    log.info("Got dt for " + fs.getUri() + "; " + token);
                }
            }
            DataOutputBuffer dob = new DataOutputBuffer();
            credentials.writeTokenStorageToStream(dob);
            ByteBuffer fsTokens = ByteBuffer.wrap(dob.getData(), 0, dob.getLength());
            amContainer.setTokens(fsTokens);
        }
        return amContainer;
    }

    private static void addToLocalResources(FileSystem fs, String fileSrcPath,
                                            String fileDstPath, String appId,
                                            Map<String, LocalResource> localResources)
            throws IllegalArgumentException, IOException {
        String suffix = "mytest" + "/" + appId + "/" + fileDstPath;
        Path dst = new Path(fs.getHomeDirectory(), suffix);
        log.info("hdfs copyFromLocalFile " + fileSrcPath + " =>" + dst);
        fs.copyFromLocalFile(new Path(fileSrcPath), dst);
        FileStatus scFileStatus = fs.getFileStatus(dst);
        LocalResource scRsrc = LocalResource.newInstance(
                ConverterUtils.getYarnUrlFromPath(dst), LocalResourceType.FILE,
                LocalResourceVisibility.APPLICATION, scFileStatus.getLen(),
                scFileStatus.getModificationTime());

        localResources.put(fileDstPath, scRsrc);

    }

    private static void monitorApplicationReport(YarnClient yarnClient, ApplicationId appId) throws YarnException, IOException {
        while (true) {
            try {
                Thread.sleep(5 * 1000);
            } catch (InterruptedException e) {

            }
            ApplicationReport report = yarnClient.getApplicationReport(appId);
            log.info("Got application report " +
                    ", clientToAMToken=" + report.getClientToAMToken()
                    + ", appDiagnostics=" + report.getDiagnostics()
                    + ", appMasterHost=" + report.getHost()
                    + ", appQueue=" + report.getQueue()
                    + ", appMasterRpcPort=" + report.getRpcPort()
                    + ", appStartTime=" + report.getStartTime()
                    + ", yarnAppState=" + report.getYarnApplicationState().toString()
                    + ", distributedFinalState=" + report.getFinalApplicationStatus().toString()
                    + ", appTrackingUrl=" + report.getTrackingUrl()
                    + ", appUser=" + report.getUser());
        }
    }

}
