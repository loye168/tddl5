package com.alibaba.cobar.manager.dataobject.xml;

public class VipDO {

    private long   id;
    private String sid;
    private long[] cobarIds;
    private String schema;
    private int[]  weights;

    public VipDO(){
    }

    public long getId() {
        return id;
    }

    public void setId(long id) {
        this.id = id;
    }

    public String getSid() {
        return sid;
    }

    public void setSid(String sid) {
        this.sid = sid;
    }

    /**
     * @return length == length of {@link #getWeights()}
     */
    public long[] getCobarIds() {
        return cobarIds;
    }

    public void setCobarIds(long[] cobarIds) {
        this.cobarIds = cobarIds;
    }

    public String getSchema() {
        return schema;
    }

    public void setSchema(String schema) {
        this.schema = schema;
    }

    /**
     * @return length == length of {@link #getCobarIds()}
     */
    public int[] getWeights() {
        return weights;
    }

    public void setWeights(int[] weights) {
        this.weights = weights;
    }

    public String idsString() {
        if (cobarIds.length < 1) {
            return "";
        }
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < cobarIds.length - 1; i++) {
            id.append(String.valueOf(cobarIds[i])).append(",");
        }
        id.append(String.valueOf(cobarIds[cobarIds.length - 1]));
        return id.toString();
    }

    public String weightsString() {
        if (weights.length < 1) {
            return "";
        }
        StringBuilder id = new StringBuilder();
        for (int i = 0; i < weights.length - 1; i++) {
            id.append(String.valueOf(weights[i])).append(",");
        }
        id.append(String.valueOf(weights[weights.length - 1]));
        return id.toString();
    }
}
