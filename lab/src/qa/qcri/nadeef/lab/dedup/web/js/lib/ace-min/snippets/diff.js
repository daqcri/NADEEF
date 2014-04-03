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

ace.define("ace/snippets/diff",["require","exports","module"],function(e,t,n){t.snippetText='# DEP-3 (http://dep.debian.net/deps/dep3/) style patch header\nsnippet header DEP-3 style header\n	Description: ${1}\n	Origin: ${2:vendor|upstream|other}, ${3:url of the original patch}\n	Bug: ${4:url in upstream bugtracker}\n	Forwarded: ${5:no|not-needed|url}\n	Author: ${6:`g:snips_author`}\n	Reviewed-by: ${7:name and email}\n	Last-Update: ${8:`strftime("%Y-%m-%d")`}\n	Applied-Upstream: ${9:upstream version|url|commit}\n\n',t.scope="diff"})