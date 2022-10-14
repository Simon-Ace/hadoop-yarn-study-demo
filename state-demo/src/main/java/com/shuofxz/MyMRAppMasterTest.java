package com.shuofxz;

import com.shuofxz.event.JobEvent;
import com.shuofxz.event.JobEventType;
import com.shuofxz.exec.MyMRAppMaster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MyMRAppMasterTest {
    public static void main(String[] args) {
        String jobID = "job_20221013_01";
        MyMRAppMaster appMaster = new MyMRAppMaster("My MRAppMaster Test", jobID);
        YarnConfiguration conf = new YarnConfiguration(new Configuration());
        try {
            appMaster.serviceInit(conf);
            appMaster.serviceStart();
        } catch (Exception e) {
            e.printStackTrace();
        }

        // 这里是异步的，发了一堆的事件到等待队列中，等 Async Dispatcher 分发处理
        // 这里发完 JOB_INIT 事件就完事了，后续是直接链式处理的
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID, JobEventType.JOB_INIT));
    }
}
