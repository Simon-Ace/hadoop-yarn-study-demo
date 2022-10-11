package com.shuofxz.event;

/**
 * Event types handled by Job.
 */
public enum JobEventType {

  //Producer:Client
  JOB_KILL,

  //Producer:MRAppMaster
  JOB_INIT
}
