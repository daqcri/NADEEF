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

define(['router', 'state'], function(Router, State) {
    var PROGRESS = 'progress';
    var PROJECT  = 'project';
    var cache = {};

    function get(call) {
        var key = arguments.callee.caller.toString().match(/function ([^\(]+)/)[1];
        var promise = cache[key];
        if (!promise) {
            promise = $.ajax(call);
            cache[key] = promise;
        } else {
            console.log("Reusing cache " + key);
        }
        return promise;
    }

    function request(promise, successCallback, failureCallback) {
        var key = arguments.callee.caller.toString().match(/function ([^\(]+)/)[1];
        $.when(promise).done(function (data) {
            successCallback(data);
        }).fail(function (data) {
            failureCallback(data);
        }).always(function () {
            delete cache[key];
        });
    }

    function getProjectName() {
        return State.getProject();
    }

    function getProgress(successCallback, failureCallback) {
        request(
            get({ url : "/progress", type: "GET" }),
            successCallback,
            failureCallback
        );
    }

    function getProject(successCallback, failureCallback) {
        request(
            get({ url : "/project", type: "GET" }),
            successCallback,
            failureCallback
        );
    }

    function deleteViolation(successCallback, failureCallback) {
        $.ajax({
            url: '/' + getProjectName() + '/table/violation',
            type: "DELETE",
            success: successCallback,
            error: failureCallback
        });
    }

    function getSource(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/source',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
    }

    function getRule(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule',
            type: 'GET',
            success: successCallback,
            error: failureCallback
        });
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

    function getTableSchema(tableName, successCallback, failureCallback) {
        request(
            get({
                url : '/' + getProjectName() + '/table/' + tableName + '/schema',
                type: 'GET'
            }),
            successCallback,
            failureCallback
        );
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

        createProject: createProject,
        createRule: createRule,
        deleteRule: deleteRule,
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
        getTupleRank: getTupleRank
    };
});