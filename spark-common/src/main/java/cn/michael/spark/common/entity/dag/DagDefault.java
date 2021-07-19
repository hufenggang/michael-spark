package cn.michael.spark.common.entity.dag;

/**
 * Created by hufenggang on 2020/3/29.
 */
public class DagDefault implements Dag {
    // DAG-ID
    protected String dagId;
    // DAG名称
    protected String dagName;
    // DAG类型
    protected String dagtype;
    // DAG描述信息
    protected String dagDesc;

    public String getDagId() {
        return dagId;
    }

    public void setDagId(String dagId) {
        this.dagId = dagId;
    }

    public String getDagName() {
        return dagName;
    }

    public void setDagName(String dagName) {
        this.dagName = dagName;
    }

    public String getDagtype() {
        return dagtype;
    }

    public void setDagtype(String dagtype) {
        this.dagtype = dagtype;
    }

    public String getDagDesc() {
        return dagDesc;
    }

    public void setDagDesc(String dagDesc) {
        this.dagDesc = dagDesc;
    }
}
