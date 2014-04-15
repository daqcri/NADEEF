/*
 * QCRI, NADEEF LICENSE
 * NADEEF is an extensible, generalized and easy-to-deploy data cleaning platform built at QCRI.
 * NADEEF means "Clean" in Arabic
 *
 * Copyright (c) 2011-2013, Qatar Foundation for Education, Science and Community Development (on
 * behalf of Qatar Computing Research Institute) having its principle place of business in Doha,
 * Qatar with the registered address P.O box 5825 Doha, Qatar (hereinafter referred to as "QCRI")
 *
 * NADEEF has patent pending nevertheless the following is granted.
 * NADEEF is released under the terms of the MIT License, (http://opensource.org/licenses/MIT).
 */

define(['router', 'state', 'config'], function(Router, State, Config) {
    var cache = {};

    function get(call) {
        var key = arguments.callee.caller.toString().match(/function ([^\(]+)/)[1];
        if (!('type' in call))
            call['type'] = 'GET';
        var promise = cache[key];
        if (!promise) {
            promise = $.ajax(call);
            cache[key] = promise;
        } else {
            console.log("Reusing cache " + key);
        }
        return promise;
    }

    function request(promise, callbacks) {
        var key = arguments.callee.caller.toString().match(/function ([^\(]+)/)[1];
        if (callbacks.before)
            callbacks.before();
        $.when(promise).done(function (data) {
            if (callbacks.success)
                callbacks.success(data);
        }).fail(function (data) {
            if (callbacks.failure)
                callbacks.failure(data);
        }).always(function (data) {
            if (callbacks.always)
                callbacks.always(data);
            delete cache[key];
        });
    }

    function getProjectName() {
        return State.get('project');
    }

    function getProgress(x) {
        request(get({ url : "/progress", type: "GET" }), x);
    }

    function getProject(x) {
        request(get({ url : "/project"}), x);
    }

    function isRuleMinerRunning() {
        // request(get({ url : Config.RuleMinerHostName}))
        return true;
    }

    function deleteViolation(successCallback, failureCallback) {
        $.ajax({
            url: '/' + getProjectName() + '/table/violation',
            type: "DELETE",
            success: successCallback,
            error: failureCallback
        });
    }

    function getSource(x) {
        request(get({ url : '/' + getProjectName() + '/data/source'}), x);
    }

    function getRule(x) {
        request(get({ url : '/' + getProjectName() + '/data/rule'}), x);
    }

    function deleteRule(ruleName, successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule/' + ruleName,
            type: 'DELETE',
            success: successCallback,
            error: failureCallback
        });
    }

    function getRuleDetail(ruleName, successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule/' + ruleName,
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getTableSchema(tableName, x) {
        request(get({url : '/' + getProjectName() + '/table/' + tableName + '/schema',}), x);
    }

    function getOverview(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/overview',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getAttribute(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/attribute',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getViolationRelation(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/violation_relation',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getRuleDistribution(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/rule',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getTupleRank(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/top10',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function doDetect(data, successCallback, failureCallback) {
        // inject project name
        data.project = getProjectName();
        $.ajax({
            url : '/do/detect',
            type: 'POST',
            dataType: 'json',
            data: data,
            success: successCallback,
            error: failureCallback
        });
    }

    function doVerify(data, successCallback, failureCallback) {
        // inject project name
        data.project = getProjectName();
        $.ajax({
            url : '/do/verify',
            type: 'POST',
            dataType: 'json',
            data: data,
            success: successCallback,
            error: failureCallback
        });
    }

    function doRepair(data, successCallback, failureCallback) {
        // inject project name
        data.project = getProjectName();
        $.ajax({
            url : '/do/repair',
            type: 'POST',
            dataType: 'json',
            data: data,
            success: successCallback,
            error: failureCallback
        });
    }

    function doGenerate(data, successCallback, failureCallback) {
        // inject project name
        data.project = getProjectName();
        $.ajax({
            url: "/do/generate",
            type: 'POST',
            data: data,
            success: successCallback,
            error: failureCallback
        });
    }

    function createRule(data, successCallback, failureCallback) {
        // inject project name
        data.project = getProjectName();
        $.ajax({
            url : "/" + getProjectName() + "/data/rule",
            type: 'POST',
            dataType: 'json',
            data: data,
            success: successCallback,
            error: failureCallback
        });
    }

    function createProject(projectName, successCallback, failureCallback) {
        $.ajax({
            url : '/project',
            type: 'POST',
            dataType: 'json',
            data: { project : projectName },
            success: successCallback,
            error: failureCallback
        });
    }

    return {
        doDetect : doDetect,
        doVerify : doVerify,
        doRepair : doRepair,
        doGenerate : doGenerate,
        deleteViolation : deleteViolation,
        deleteRule: deleteRule,
        createProject: createProject,
        createRule: createRule,
        getProgress : getProgress,
        getProject: getProject,
        getSource: getSource,
        getRule: getRule,
        getRuleDetail: getRuleDetail,
        getTableSchema: getTableSchema,
        getOverview: getOverview,
        getAttribute: getAttribute,
        getViolationRelation: getViolationRelation,
        getRuleDistribution: getRuleDistribution,
        getTupleRank: getTupleRank,
        isRuleMinerRunning: isRuleMinerRunning
    };
});