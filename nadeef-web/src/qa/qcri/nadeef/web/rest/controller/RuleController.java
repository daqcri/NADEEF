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

package qa.qcri.nadeef.web.rest.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.*;
import qa.qcri.nadeef.web.rest.model.Rule;
import qa.qcri.nadeef.web.rest.model.RuleDao;

import java.util.List;

@RestController
public class RuleController {
    private RuleDao ruleDao;

    @Autowired
    public RuleController(RuleDao ruleDao) {
        this.ruleDao = ruleDao;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{project}/data/rule")
    public @ResponseBody List<Rule> getRules(@PathVariable String project) {
        return ruleDao.getRules(project);
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{project}/data/rule/{ruleName}")
    public @ResponseBody Rule getRule(@PathVariable String project, @PathVariable String ruleName) {
        return ruleDao.queryRule(ruleName, project);
    }

    @RequestMapping(method = RequestMethod.DELETE, value = "/{project}/data/rule/{ruleName}")
    @ResponseStatus(HttpStatus.OK)
    public void deleteRule(@PathVariable String project, @PathVariable String ruleName) {
        ruleDao.deleteRule(ruleName, project);
    }

    @RequestMapping(method = RequestMethod.POST, value = "/{project}/data/rule")
    @ResponseStatus(HttpStatus.CREATED)
    public void createRule(@PathVariable String project, @RequestBody Rule rule) {
        ruleDao.insertRule(rule, project);
    }
}
