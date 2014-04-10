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

define([], function() {

    var PROJECT = "project";
    var JOB     = "job";
    var SOURCE  = "source";
    var RULE    = "rule";

    function setProject(projectName) {
        window.localStorage.setItem(PROJECT, projectName);
    }

    function getProject() {
        if (_.isUndefined(window.localStorage.getItem(PROJECT)))
            return null;
        return window.localStorage.getItem(PROJECT);
    }

    function setJob(jobList) {
        window.localStorage.setItem(JOB, JSON.stringify(jobList));
    }

    function addJob(job) {
        var jobList = getJob();
        jobList.push(job);
        setJob(jobList);
    }

    function getJob() {
        if (_.isUndefined(window.localStorage.getItem(JOB)))
            return null;
        return JSON.parse(window.localStorage.getItem(JOB));
    }

    function setSource(sourceList) {
        window.localStorage.setItem(SOURCE, JSON.stringify(sourceList));
    }

    function getSource() {
        if (_.isUndefined(window.localStorage.getItem(SOURCE)))
            return null;
        return JSON.parse(window.localStorage.getItem(SOURCE));
    }

    function getRule() {
        if (_.isUndefined(window.localStorage.getItem(RULE)))
            return null;
        return JSON.parse(window.localStorage.getItem(RULE));
    }

    function setRule(ruleList) {
        window.localStorage.setItem(RULE, JSON.stringify(ruleList));
    }

    function init() {
        setJob([]);
        setRule([]);
        setSource([]);
    }

    return {
        init : init,
        setProject : setProject,
        getProject : getProject,
        getJob : getJob,
        setJob : setJob,
        addJob : addJob,
        setSource : setSource,
        getSource : getSource,
        getRule : getRule,
        setRule : setRule
    };
})
