package com.bushpath.anamnesis.ipc.rpc;

public class SocketContext {
    protected String effectiveUser;
    protected String realUser;
    protected String protocol;

    public SocketContext() {
        this.effectiveUser = "";
        this.realUser = "";
        this.protocol = "";
    }

    public String getEffectiveUser() {
        return this.effectiveUser;
    }

    public void setEffectiveUser(String effectiveUser) {
        this.effectiveUser = effectiveUser;
    }

    public String getRealUser() {
        return this.realUser;
    }

    public void setRealUser(String realUser) {
        this.realUser = realUser;
    }

    public String getProtocol() {
        return this.protocol;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }
}
