define(function(require) {

  var Visualization = require("Visualization");

  function KeyValueSequence() {}

  KeyValueSequence.prototype = new Visualization();
  KeyValueSequence.prototype.constructor = Visualization;
  KeyValueSequence.prototype.parent = Visualization.prototype;

  KeyValueSequence.prototype._draw = function(keyValueObject) {
    var Util = require("util"),
      d3 = require("d3");

    var contentId = this._content.id;
    var table = Util.generateElement(this._content, contentId + "-listing", "table");
    this._content.style.display = "table-cell";
    this._content.style.verticalAlign = "middle";
    this._content.style.textAlign = "center";
    this._content.style.width = "5000px";
    table.setAttribute("class", "keyValueTable");
    var keyValueArray = Object.keys(keyValueObject)
      .map(function(key) {
        return [{
          entry: key,
          class: "key"
        }, {
          entry: keyValueObject[key],
          class: "value"
        }];
      });
    var rows = d3.select(table)
      .selectAll("tr")
      .data(keyValueArray)
      .enter()
      .append("tr");
    rows.selectAll("td")
      .data(function(row) {
        return row;
      })
      .enter()
      .append("td")
      .text(function(cell) {
        return cell.entry;
      })
      .attr("class", function(cell) {
        return cell.class;
      });
  }

  KeyValueSequence.prototype._clear = function() {}

  return KeyValueSequence;

});
