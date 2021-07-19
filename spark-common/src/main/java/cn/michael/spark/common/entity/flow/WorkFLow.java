package cn.michael.spark.common.entity.flow;

import java.util.Date;

/**
 * Created by hufenggang on 2020/3/28.
 *
 * 工作流
 */
public class WorkFLow extends FlowDefault {
    // 工作流开始时间
    protected Date startTime;
    // 工作流结束时间
    protected Date endTime;
    // 工作流运行时常
    protected Long duration;

    @Override
    public String getFlowId() {
        return super.getFlowId();
    }

    @Override
    public void setFlowId(String flowId) {
        super.setFlowId(flowId);
    }

    @Override
    public String getFlowName() {
        return super.getFlowName();
    }

    @Override
    public void setFlowName(String flowName) {
        super.setFlowName(flowName);
    }

    @Override
    public String getFlowtype() {
        return super.getFlowtype();
    }

    @Override
    public void setFlowtype(String flowtype) {
        super.setFlowtype(flowtype);
    }

    @Override
    public String getFlowDesc() {
        return super.getFlowDesc();
    }

    @Override
    public void setFlowDesc(String flowDesc) {
        super.setFlowDesc(flowDesc);
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
