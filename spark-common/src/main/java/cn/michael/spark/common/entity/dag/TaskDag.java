package cn.michael.spark.common.entity.dag;

import java.util.Date;

/**
 * Created by hufenggang on 2020/3/29.
 *
 * 任务DAG
 */
public class TaskDag extends DagDefault {
    // 开始时间
    protected Date startTime;
    // 结束时间
    protected Date endTime;
    // 运行时常
    protected Long duration;

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
