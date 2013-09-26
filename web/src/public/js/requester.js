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
        if (state == null || _.isUndefined(state.projectName) || state.projectName == '') {
            Router.redirectToRoot();
        }
        return projectName;
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


    return {
        deleteViolation : deleteViolation,
        getSource: getSource
    };
});