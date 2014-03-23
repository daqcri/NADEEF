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

define(['router'], function(Router) {
    function getProjectName() {
        var state = window.history.state;
        if (state == null || _.isUndefined(state.name) || state.name == '') {
            Router.redirectToRoot();
        }
        return state.name;
    }

    function getProject(successCallback, failureCallback) {
        $.ajax({
            url: '/project',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function deleteViolation(successCallback, failureCallback) {
        $.ajax({
            url: '/' + getProjectName() + '/table/violation',
            type: "DELETE",
            success: successCallback,
            fail: failureCallback
        });
    }

    function getSource(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/source',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getRule(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function deleteRule(ruleName, successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule/' + ruleName,
            type: 'DELETE',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getRuleDetail(ruleName, successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/data/rule/' + ruleName,
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getTableSchema(tableName, successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/table/' + tableName + '/schema',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getOverview(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/overview',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getAttribute(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/attribute',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getViolationRelation(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/violation_relation',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getRuleDistribution(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/rule',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
        });
    }

    function getTupleRank(successCallback, failureCallback) {
        $.ajax({
            url : '/' + getProjectName() + '/widget/top10',
            type: 'GET',
            success: successCallback,
            fail: failureCallback
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
            fail: failureCallback
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
            fail: failureCallback
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
            fail: failureCallback
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
            fail: failureCallback
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
            fail: failureCallback
        });
    }

    function createProject(projectName, successCallback, failureCallback) {
        $.ajax({
            url : '/project',
            type: 'POST',
            dataType: 'json',
            data: { project : projectName },
            success: successCallback,
            fail: failureCallback
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