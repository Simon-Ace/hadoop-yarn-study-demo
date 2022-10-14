package com.shuofxz;

import com.shuofxz.event.JobEvent;
import com.shuofxz.event.JobEventType;
import org.apache.hadoop.yarn.event.EventHandler;
import org.apache.hadoop.yarn.state.*;

import java.util.EnumSet;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/*
* 可参考 Yarn 中实现的状态机对象：
* ResourceManager 中的 RMAppImpl、RMApp- AttemptImpl、RMContainerImpl 和 RMNodeImpl，
* NodeManager 中 的 ApplicationImpl、 ContainerImpl 和 LocalizedResource，
* MRAppMaster 中的 JobImpl、TaskImpl 和 TaskAttemptImpl 等
* */
@SuppressWarnings({"rawtypes", "unchecked"})
public class JobStateMachine implements EventHandler<JobEvent> {
    private final String jobID;
    private EventHandler eventHandler;
    private final Lock writeLock;
    private final Lock readLock;

    // 定义状态机
    protected static final StateMachineFactory<JobStateMachine, JobStateInternal,
            JobEventType, JobEvent>
            stateMachineFactory = new StateMachineFactory<JobStateMachine, JobStateInternal, JobEventType, JobEvent>(JobStateInternal.NEW)
            .addTransition(JobStateInternal.NEW, JobStateInternal.INITED, JobEventType.JOB_INIT, new InitTransition())
            .addTransition(JobStateInternal.INITED, JobStateInternal.SETUP, JobEventType.JOB_START, new StartTransition())
            .addTransition(JobStateInternal.SETUP, JobStateInternal.RUNNING, JobEventType.JOB_SETUP_COMPLETED, new SetupCompletedTransition())
            .addTransition(JobStateInternal.RUNNING, EnumSet.of(JobStateInternal.KILLED, JobStateInternal.SUCCEEDED), JobEventType.JOB_COMPLETED, new JobTasksCompletedTransition())
            .installTopology();

    private final StateMachine<JobStateInternal, JobEventType, JobEvent> stateMachine;

    public JobStateMachine(String jobID, EventHandler eventHandler) {
        this.jobID = jobID;

        // 多线程异步处理，state 有可能被同时读写，使用读写锁来避免竞争
        ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
        this.readLock = readWriteLock.readLock();
        this.writeLock = readWriteLock.writeLock();

        this.eventHandler = eventHandler;
        stateMachine = stateMachineFactory.make(this);
    }

    protected StateMachine<JobStateInternal, JobEventType, JobEvent> getStateMachine() {
        return stateMachine;
    }

    public static class InitTransition implements SingleArcTransition<JobStateMachine, JobEvent> {
        @Override
        public void transition(JobStateMachine jobStateMachine, JobEvent jobEvent) {
            System.out.println("Receiving event " + jobEvent);
            // do something...
            // 完成后发送新的 Event —— JOB_START
            jobStateMachine.eventHandler.handle(new JobEvent(jobStateMachine.jobID, JobEventType.JOB_START));
        }
    }

    public static class StartTransition implements SingleArcTransition<JobStateMachine, JobEvent> {
        @Override
        public void transition(JobStateMachine jobStateMachine, JobEvent jobEvent) {
            System.out.println("Receiving event " + jobEvent);
            jobStateMachine.eventHandler.handle(new JobEvent(jobStateMachine.jobID, JobEventType.JOB_SETUP_COMPLETED));
        }
    }

    public static class SetupCompletedTransition implements SingleArcTransition<JobStateMachine, JobEvent> {
        @Override
        public void transition(JobStateMachine jobStateMachine, JobEvent jobEvent) {
            System.out.println("Receiving event " + jobEvent);
            jobStateMachine.eventHandler.handle(new JobEvent(jobStateMachine.jobID, JobEventType.JOB_COMPLETED));
        }
    }

    public static class JobTasksCompletedTransition implements MultipleArcTransition<JobStateMachine, JobEvent, JobStateInternal> {
        @Override
        public JobStateInternal transition(JobStateMachine jobStateMachine, JobEvent jobEvent) {
            System.out.println("Receiving event " + jobEvent);

            // 这是多结果状态部分，因此需要人为制定后续状态
            // 这里整个流程结束，设置一下对应的状态
            boolean flag = true;
            if (flag) {
                return JobStateInternal.SUCCEEDED;
            } else {
                return JobStateInternal.KILLED;
            }
        }
    }

    @Override
    public void handle(JobEvent jobEvent) {
        try {
            // 注意这里为了避免静态条件，使用了读写锁
            writeLock.lock();
            JobStateInternal oldState = getInternalState();
            try {
                getStateMachine().doTransition(jobEvent.getType(), jobEvent);
            } catch (InvalidStateTransitionException e) {
                System.out.println("Can't handle this event at current state!");
            }
            if (oldState != getInternalState()) {
                System.out.println("Job Transitioned from " + oldState + " to " + getInternalState());
            }

        } finally {
            writeLock.unlock();
        }
    }

    public JobStateInternal getInternalState() {
        readLock.lock();
        try {
            return getStateMachine().getCurrentState();
        } finally {
            readLock.unlock();
        }
    }

    public enum JobStateInternal {
        NEW,
        SETUP,
        INITED,
        RUNNING,
        SUCCEEDED,
        KILLED
    }
}
