package cn.michael.spark.common.entity.task;

import java.util.Date;

/**
 * Created by hufenggang on 2020/3/28.
 *
 * 任务接口实现类
 */
public class TaskDefault implements Task {
    // 任务ID
    protected String taskId;
    // 任务名称
    protected String taskName;
    // 任务类型
    protected String tasktype;
    // 任务描述信息
    protected String taskDesc;
    // 任务开始时间
    protected Date startTime;
    // 任务结束时间
    protected Date endTime;
    // 任务运行时常
    protected Long duration;

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskName() {
        return taskName;
    }

    public void setTaskName(String taskName) {
        this.taskName = taskName;
    }

    public String getTasktype() {
        return tasktype;
    }

    public void setTasktype(String tasktype) {
        this.tasktype = tasktype;
    }

    public String getTaskDesc() {
        return taskDesc;
    }

    public void setTaskDesc(String taskDesc) {
        this.taskDesc = taskDesc;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public Long getDuration() {
        return duration;
    }

    public void setDuration(Long duration) {
        this.duration = duration;
    }
}
