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
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import qa.qcri.nadeef.web.rest.dao.ProjectDao;

import java.util.List;

@Controller
public class ProjectController {
    private ProjectDao projectDao;

    @Autowired
    public ProjectController(ProjectDao projectDao) {
        this.projectDao = projectDao;
    }

    @RequestMapping(method = RequestMethod.GET, value = "/project")
    public @ResponseBody List<String> getProjects() {
        return projectDao.getProjects();
    }

    @RequestMapping(method = RequestMethod.POST, value = "/project")
    @ResponseStatus(HttpStatus.CREATED)
    public @ResponseBody void createProject(@RequestBody String name) {
        projectDao.create(name);
    }
}
