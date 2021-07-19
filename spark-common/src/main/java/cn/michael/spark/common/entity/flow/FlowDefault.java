package cn.michael.spark.common.entity.flow;

/**
 * Created by hufenggang on 2020/3/28.
 * 
 * Flow接口实现
 */
public class FlowDefault implements Flow {
    // 流ID
    protected String flowId;
    // 流名称
    protected String flowName;
    // 流类型
    protected String flowtype;
    // 流描述信息
    protected String flowDesc;

    public String getFlowId() {
        return flowId;
    }

    public void setFlowId(String flowId) {
        this.flowId = flowId;
    }

    public String getFlowName() {
        return flowName;
    }

    public void setFlowName(String flowName) {
        this.flowName = flowName;
    }

    public String getFlowtype() {
        return flowtype;
    }

    public void setFlowtype(String flowtype) {
        this.flowtype = flowtype;
    }

    public String getFlowDesc() {
        return flowDesc;
    }

    public void setFlowDesc(String flowDesc) {
        this.flowDesc = flowDesc;
    }
}
