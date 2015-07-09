function Empty() {}

Empty.prototype = new Visualization();
Empty.prototype.constructor = Visualization;
Empty.prototype.parent = Visualization.prototype;

Empty.prototype._draw = function() {}

Empty.prototype._clear = function() {}
