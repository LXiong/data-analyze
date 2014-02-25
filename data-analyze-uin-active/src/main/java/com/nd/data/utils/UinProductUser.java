package com.nd.data.utils;

/**
 * Created with IntelliJ IDEA. User: Lin QiLi Date: 14-2-20 Time: 下午5:54
 */
public class UinProductUser {

    private String _firstDay;
    private String _lastDay;
    private String _product;
    private String _channelId;
    private String _platForm;
    private String _productVersion;
    private String _loginCnt;           // only for the update loginCnt;
    private String _intervalDays;

    public String get_firstDay() {
        return _firstDay;
    }

    public void set_firstDay(String _firstDay) {
        this._firstDay = _firstDay;
    }

    public String get_lastDay() {
        return _lastDay;
    }

    public void set_lastDay(String _lastDay) {
        this._lastDay = _lastDay;
    }

    public String get_product() {
        return _product;
    }

    public void set_product(String _product) {
        this._product = _product;
    }

    public String get_channelId() {
        return _channelId;
    }

    public void set_channelId(String _channelId) {
        this._channelId = _channelId;
    }

    public String get_platForm() {
        return _platForm;
    }

    public void set_platForm(String _platForm) {
        this._platForm = _platForm;
    }

    public String get_productVersion() {
        return _productVersion;
    }

    public void set_productVersion(String _productVersion) {
        this._productVersion = _productVersion;
    }

    public String get_loginCnt() {
        return _loginCnt;
    }

    public void set_loginCnt(String _loginCnt) {
        this._loginCnt = _loginCnt;
    }

    public String get_intervalDays() {
        return _intervalDays;
    }

    public void set_intervalDays(String intervalDays) {
        this._intervalDays = intervalDays;
    }
}