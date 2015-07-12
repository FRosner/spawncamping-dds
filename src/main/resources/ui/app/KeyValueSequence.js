function KeyValueSequence() {}

KeyValueSequence.prototype = new Visualization();
KeyValueSequence.prototype.constructor = Visualization;
KeyValueSequence.prototype.parent = Visualization.prototype;

KeyValueSequence.prototype._draw = function(keyValueObject) {
  var contentId = this._content.id;
  var table = generateElement(this._content, contentId + "-listing", "table");
  table.setAttribute("class", "keyValueTable");
  var keyValueArray = Object.keys(keyValueObject).map(function(key) {
    return [{entry : key, class : "key"}, {entry : keyValueObject[key], class : "value"}];
  });
  var rows = d3.select(table).selectAll("tr").data(keyValueArray).enter().append("tr");
  rows.selectAll("td").data(function(row) {return row;}).enter().append("td").text(function(cell) {return cell.entry;})
  .attr("class", function(cell) {return cell.class;});
}

KeyValueSequence.prototype._clear = function() {}
