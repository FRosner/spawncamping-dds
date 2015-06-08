function Composite() {}

Composite.prototype = new Visualization();
Composite.prototype.constructor = Visualization;
Composite.prototype.parent = Visualization.prototype;

Composite.prototype._draw = function(composite) {
  var servedComponents = [];
  composite = composite.map(function(row, rowIdx) {
    return row.map(function(cell, cellIdx) {
      cell.containerId = "container-" + rowIdx + "-" + cellIdx;
      cell.contentId = "content-" + rowIdx + "-" + cellIdx;
      cell.headerId = "header-" + rowIdx + "-" + cellIdx;
      return cell;
    });
  });
  var container = d3.select(this._content)
    .append("div")
    .attr("class", "container-fluid");
  var rows = container.selectAll("div")
    .data(composite)
    .enter()
    .append("div")
    .attr("class", "bootstrap-div row");
  var cells = rows.selectAll("div")
    .data(function(row) {
      if (row.length > 12) {
        console.warn("The following row has more than 12 cells which is not properly supported by grid layout: " + JSON.stringify(row));
      }
      var enhancedRow = row.map(function(cell) {
        var enhancedCell = cell;
        var columnLayout = "bootstrap-div col-lg-"
        if (row.length == 1) {
          enhancedCell.cssClass = columnLayout + "12";
        } else if (row.length == 2) {
          enhancedCell.cssClass = columnLayout + "6";
        } else if (row.length == 3) {
          enhancedCell.cssClass = columnLayout + "4";
        } else if (row.length == 4) {
          enhancedCell.cssClass = columnLayout + "3";
        } else if (row.length <= 6) {
          enhancedCell.cssClass = columnLayout + "2";
        } else {
          enhancedCell.cssClass = columnLayout + "1";
        }
        return enhancedCell;
      });
    return enhancedRow;
  }).enter();
  var container = cells.append("div")
  .attr("class", function(cell) {
    return cell.cssClass;
  })
  .attr("id", function(cell) {
    return cell.containerId;
  });
  var header = container.append("div")
  .attr("class", "header")
  .attr("id", function(cell) {
    return cell.headerId;
  });
  var content = container.append("div")
  .attr("id", function(cell) {
    return cell.contentId;
  }).style({height: 500})
  .each(function(cell) {
    var servable = cell;
    var contentId = cell.contentId;
    var headerId = cell.headerId;
    servedComponents.push(drawServable(cell, headerId, contentId));
  });
  this._servedComponents = servedComponents;
}

Composite.prototype.clearHeader = function() {
}
