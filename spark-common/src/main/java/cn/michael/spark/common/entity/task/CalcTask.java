package cn.michael.spark.common.entity.task;

import java.util.ArrayList;
import java.util.Date;

/**
 * Created by hufenggang on 2020/3/28.
 * <p>
 * 计算任务
 */
public class CalcTask extends TaskDefault {
    // 依赖任务个数
    protected Integer depTaskCount;
    // 依赖计算任务
    protected ArrayList<CalcTask> depTasks;

    @Override
    public String getTaskId() {
        return super.getTaskId();
    }

    @Override
    public void setTaskId(String taskId) {
        super.setTaskId(taskId);
    }

    @Override
    public String getTaskName() {
        return super.getTaskName();
    }

    @Override
    public void setTaskName(String taskName) {
        super.setTaskName(taskName);
    }

    @Override
    public String getTasktype() {
        return super.getTasktype();
    }

    @Override
    public void setTasktype(String tasktype) {
        super.setTasktype(tasktype);
    }

    @Override
    public String getTaskDesc() {
        return super.getTaskDesc();
    }

    @Override
    public void setTaskDesc(String taskDesc) {
        super.setTaskDesc(taskDesc);
    }

    @Override
    public Date getStartTime() {
        return super.getStartTime();
    }

    @Override
    public void setStartTime(Date startTime) {
        super.setStartTime(startTime);
    }

    @Override
    public Date getEndTime() {
        return super.getEndTime();
    }

    @Override
    public void setEndTime(Date endTime) {
        super.setEndTime(endTime);
    }

    @Override
    public Long getDuration() {
        return super.getDuration();
    }

    @Override
    public void setDuration(Long duration) {
        super.setDuration(duration);
    }

    /**
     * 添加依赖任务
     *
     * @param task
     */
    public void addDepTask(CalcTask task) {
        depTasks.add(task);
    }

    /**
     * 删除依赖任务
     *
     * @param task
     */
    public void removeDepTask(CalcTask task) {
        depTasks.remove(task);
    }

    public Integer getDepTaskCount() {
        return depTaskCount;
    }

    public void setDepTaskCount(Integer depTaskCount) {
        this.depTaskCount = depTaskCount;
    }
}
