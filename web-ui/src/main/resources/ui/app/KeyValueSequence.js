define(function(require) {

  var Visualization = require("Visualization");

  function KeyValueSequence() {}

  KeyValueSequence.prototype = new Visualization();
  KeyValueSequence.prototype.constructor = Visualization;
  KeyValueSequence.prototype.parent = Visualization.prototype;

  KeyValueSequence.prototype._draw = function(servable) {
    var dds = require("dds");
    this._chartDiv = dds.key_value_sequence(servable.title, servable.keyValuePairs);
    this._content.appendChild(this._chartDiv);
  }

  KeyValueSequence.prototype._clear = function() {}

  return KeyValueSequence;

});
