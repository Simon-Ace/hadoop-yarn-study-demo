package com.shuofxz;

import com.shuofxz.event.JobEvent;
import com.shuofxz.event.JobEventType;
import com.shuofxz.exec.MyMRAppMaster;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.conf.YarnConfiguration;

public class MyMRAppMasterTest {
    public static void main(String[] args) {
        String jobID = "job_20221011_99";
        MyMRAppMaster appMaster = new MyMRAppMaster("My MRAppMaster Test", jobID, 10);
        YarnConfiguration conf = new YarnConfiguration(new Configuration());
        try {
            appMaster.serviceInit(conf);
            appMaster.serviceStart();
        } catch (Exception e) {
            e.printStackTrace();
        }
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID, JobEventType.JOB_KILL));
        appMaster.getDispatcher().getEventHandler().handle(new JobEvent(jobID, JobEventType.JOB_INIT));
    }
}
