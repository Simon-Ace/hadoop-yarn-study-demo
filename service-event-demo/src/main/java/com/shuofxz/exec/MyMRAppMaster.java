package com.shuofxz.exec;

import com.shuofxz.event.JobEvent;
import com.shuofxz.event.JobEventType;
import com.shuofxz.event.TaskEvent;
import com.shuofxz.event.TaskEventType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.service.CompositeService;
import org.apache.hadoop.service.Service;
import org.apache.hadoop.yarn.event.AsyncDispatcher;
import org.apache.hadoop.yarn.event.Dispatcher;
import org.apache.hadoop.yarn.event.EventHandler;

@SuppressWarnings("unchecked")
public class MyMRAppMaster extends CompositeService {
    private Dispatcher dispatcher;  // AsyncDispatcher
    private String jobID;
    private int taskNumber;         // 一个 job 包含的 task 数
    private String[] taskIDs;

    public MyMRAppMaster(String name, String jobID, int taskNumber) {
        super(name);
        this.jobID = jobID;
        this.taskNumber = taskNumber;
        taskIDs = new String[taskNumber];
        for (int i = 0; i < taskNumber; i++) {
            taskIDs[i] = this.jobID + "_task_" + i;
        }
    }

    public void serviceInit(Configuration conf) throws Exception {
        dispatcher = new AsyncDispatcher();
        dispatcher.register(JobEventType.class, new JobEventDispatcher()); // register a job
        dispatcher.register(TaskEventType.class, new TaskEventDispatcher()); // register a task
        addService((Service) dispatcher);
        super.serviceInit(conf);
    }

    public void serviceStart() throws Exception {
        super.serviceStart();
    }

    public Dispatcher getDispatcher() {
        return dispatcher;
    }

    private class JobEventDispatcher implements EventHandler<JobEvent> {
        public void handle(JobEvent event) {
            if (event.getType() == JobEventType.JOB_KILL) {
                System.out.println("Receive JOB_KILL event, killing all the tasks");
                for (int i = 0; i < taskNumber; i++) {
                    dispatcher.getEventHandler().handle(new TaskEvent(taskIDs[i], TaskEventType.T_KILL));
                }
            } else if (event.getType() == JobEventType.JOB_INIT) {
                System.out.println("Receive JOB_INIT event, scheduling tasks");
                for (int i = 0; i < taskNumber; i++) {
                    dispatcher.getEventHandler().handle(new TaskEvent(taskIDs[i], TaskEventType.T_SCHEDULE));
                }
            }
        }
    }

    private class TaskEventDispatcher implements EventHandler<TaskEvent> {
        public void handle(TaskEvent event) {
            if (event.getType() == TaskEventType.T_KILL) {
                System.out.println("Receive T_KILL event of task id " + event.getTaskID());
            } else if (event.getType() == TaskEventType.T_SCHEDULE) {
                System.out.println("Receive T_SCHEDULE event of task id " + event.getTaskID());
            }
        }
    }
}