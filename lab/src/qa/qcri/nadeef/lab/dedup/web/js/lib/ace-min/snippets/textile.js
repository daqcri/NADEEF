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

ace.define("ace/snippets/textile",["require","exports","module"],function(e,t,n){t.snippetText='# Jekyll post header\nsnippet header\n	---\n	title: ${1:title}\n	layout: post\n	date: ${2:date} ${3:hour:minute:second} -05:00\n	---\n\n# Image\nsnippet img\n	!${1:url}(${2:title}):${3:link}!\n\n# Table\nsnippet |\n	|${1}|${2}\n\n# Link\nsnippet link\n	"${1:link text}":${2:url}\n\n# Acronym\nsnippet (\n	(${1:Expand acronym})${2}\n\n# Footnote\nsnippet fn\n	[${1:ref number}] ${3}\n\n	fn$1. ${2:footnote}\n	\n',t.scope="textile"})