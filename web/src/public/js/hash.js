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

/*
 * A simple map collection.
 */
// TODO: change to a class object
define([], function() {
    function Map() {
        this.keys = [];
        this.values = [];
    }

    Map.prototype.size = function() {
        return this.keys.length;
    }

    Map.prototype.empty = function() {
        this.keys = [];
        this.values = [];
    }

    Map.prototype.put = function(key, value) {
        for(var i = 0; i < this.keys.length; i ++) {
            if (this.keys[i] == key) {
                this.values[i] = value;
                return;
            }
        }

        this.keys.push(key);
        this.values.push(value);
    }

    Map.prototype.remove = function(key) {
        for (var i = 0; i < this.keys.length; i ++) {
            if (this.keys[i] == key) {
                this.keys.pop(i);
                this.values.pop(i);
                return true;
            }
        }
        return false;
    }

    Map.prototype.get = function(key) {
        for (var i = 0; i < this.keys.length; i ++) {
            if (this.keys[i] === key) {
                return this.values[i];
            }
        }
        return null;
    }

    Map.prototype.hasKey = function(key) {
        for (var i = 0; i < this.keys.length; i ++) {
            if (this.keys[i] == key) {
                return true;
            }
        }
        return false;
    }

    return {
        Map: Map
    };
});
