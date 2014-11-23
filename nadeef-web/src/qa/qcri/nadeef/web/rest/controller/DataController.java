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

import com.google.common.base.Strings;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;
import qa.qcri.nadeef.web.rest.dao.DataDao;

@Controller
public class DataController {
    private DataDao tableDao;

    @Autowired
    public DataController(DataDao tableDao) {
        this.tableDao = tableDao;
    }

    @RequestMapping(
        method = RequestMethod.GET,
        value = "/{projectName}/table/{name}",
        produces = "application/json; charset=utf-8"
    )
    public @ResponseBody String queryData(
        @PathVariable String projectName,
        @PathVariable String name,
        @RequestParam(value = "start", defaultValue = "0") Integer start,
        @RequestParam(value = "length", defaultValue = "10") Integer interval,
        @RequestParam(value = "search[value]", required = false, defaultValue = "") String filter,
        @RequestParam(value = "sEcho", required = false) String sEcho) {

        JsonObject json = tableDao.query(projectName, name, start, interval, filter);
        int count = json.get("data").getAsJsonArray().size();
        json.add("iTotalRecords", new JsonPrimitive(tableDao.count(projectName, name)));
        json.add("iTotalDisplayRecords", new JsonPrimitive(count));
        if (!Strings.isNullOrEmpty(sEcho))
            json.add("sEcho", new JsonPrimitive(sEcho));
        return json != null ? json.toString() : null;
    }
}
