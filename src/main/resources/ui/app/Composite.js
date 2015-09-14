define(function(require) {

  var Visualization = require("Visualization");

  function Composite(id) {
    this.id = id;
  }

  Composite.prototype = new Visualization();
  Composite.prototype.constructor = Visualization;
  Composite.prototype.parent = Visualization.prototype;

  Composite.prototype._draw = function(composite) {
    var $ = require("jquery"),
      Util = require("util"),
      d3 = require("d3"),
      Drawer = require("draw");

    var id = this.id;

    var servedComponents = [];
    var thisContent = this._content;
    composite = composite.map(function(row, rowIdx) {
      return row.map(function(cell, cellIdx) {
        cell.containerId = "container-" + thisContent.id + "-" + rowIdx + "-" + cellIdx;
        cell.contentId = "content-" + thisContent.id + "-" + rowIdx + "-" + cellIdx;
        cell.headerId = "header-" + thisContent.id + "-" + rowIdx + "-" + cellIdx;
        cell.contentWidth = $(thisContent)
          .width();
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
          console.warn("The following row has more than 12 cells which is not properly supported by grid layout: " +
            JSON.stringify(row));
        }
        var enhancedRow = row.map(function(cell) {
          var enhancedCell = cell;
          var columnLayout = "bootstrap-div col-lg-"
          if (row.length == 1) {
            enhancedCell.cssClass = columnLayout + "12";
          } else if (row.length == 2) {
            enhancedCell.cssClass = columnLayout + "6";
            enhancedCell.contentWidth = enhancedCell.contentWidth / 2
          } else if (row.length == 3) {
            enhancedCell.cssClass = columnLayout + "4";
            enhancedCell.contentWidth = enhancedCell.contentWidth / 3
          } else if (row.length == 4) {
            enhancedCell.cssClass = columnLayout + "3";
            enhancedCell.contentWidth = enhancedCell.contentWidth / 4
          } else if (row.length <= 6) {
            enhancedCell.cssClass = columnLayout + "2";
            enhancedCell.contentWidth = enhancedCell.contentWidth / 6
          } else {
            enhancedCell.cssClass = columnLayout + "1";
            enhancedCell.contentWidth = enhancedCell.contentWidth / 12
          }
          return enhancedCell;
        });
        return enhancedRow;
      })
      .enter();
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
      })
      .attr("style", function(cell) {
        return "height: " + Math.ceil(cell.contentWidth / 16 * 9) + "px"
      })
      .each(function(cell) {
        var servable = cell;
        // TODO this will not be required anymore once the id is part of the servable
        // TODO also then we don't need to hack the id hopefully...
        servable.id = id + cell.contentId;
        var contentId = cell.contentId;
        var headerId = cell.headerId;
        servedComponents.push(Drawer.drawServable(servable, headerId, contentId));
      });
    this._servedComponents = servedComponents;
  }

  Composite.prototype._clear = function() {}

  return Composite;

});
