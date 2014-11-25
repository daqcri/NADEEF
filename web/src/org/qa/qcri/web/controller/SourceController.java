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

package org.qa.qcri.web.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import org.qa.qcri.web.dao.SourceDao;
import org.qa.qcri.web.model.Source;

import java.util.List;

@Controller
public class SourceController {
    private SourceDao sourceDao;

    @Autowired
    public SourceController(SourceDao sourceDao) {
        this.sourceDao = sourceDao;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/{project}/data/source")
    public @ResponseBody List<Source> getSources(String project) {
        return sourceDao.getSources(project);
    }
}
