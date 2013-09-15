/*
 * A simple map collection.
 */
// TODO: change to a class object
define(['underscore'], function() {
	function Map() {
		this.keys = new Array();
		this.values = new Array();
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

	return Map;
});
