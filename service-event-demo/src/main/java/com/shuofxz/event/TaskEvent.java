package com.shuofxz.event;

import org.apache.hadoop.yarn.event.AbstractEvent;

/**
 * this class encapsulates task related events.
 *
 */
public class TaskEvent extends AbstractEvent<TaskEventType> {

  private String taskID;

  public TaskEvent(String taskID, TaskEventType type) {
    super(type);
    this.taskID = taskID;
  }

  public String getTaskID() {
    return taskID;
  }
}
