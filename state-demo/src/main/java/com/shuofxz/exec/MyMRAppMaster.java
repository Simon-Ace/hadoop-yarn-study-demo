package com.shuofxz.exec;

import com.shuofxz.JobStateMachine;
import com.shuofxz.event.JobEventType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;

@SuppressWarnings("unchecked")
public class MyMRAppMaster extends CompositeService {
    private Dispatcher dispatcher;  // AsyncDispatcher
    private String jobID;

    public MyMRAppMaster(String name, String jobID) {
        super(name);
        this.jobID = jobID;
    }

    public void serviceInit(Configuration conf) throws Exception {
        dispatcher = new AsyncDispatcher();
        dispatcher.register(JobEventType.class, new JobStateMachine(this.jobID, dispatcher.getEventHandler())); // register a job
        addService((Service) dispatcher);
        super.serviceInit(conf);
    }

    public void serviceStart() throws Exception {
        super.serviceStart();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

}