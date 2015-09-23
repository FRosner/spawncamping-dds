define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util"),
    Cache = require("Cache");

  require("slickgrid");
  require("slickdataview");
  require("slickpager");

  function Table(id) {
    this.id = id;
    if (Cache.existsConfig(id)) {
      this.config = Cache.getConfig(id);
    } else {
      this.config = {
        coloredDimension: null,
        tickLabelsHidden: false,
        jitterEnabled: false
      };
      Cache.setConfig(id, this.config);
    }
  };

  Table.prototype = new Visualization();
  Table.prototype.constructor = Visualization;
  Table.prototype.parent = Visualization.prototype;

  Table.prototype._draw = function(tableAndTypes) {
    var d3 = require("d3"),
      c3 = require("c3"),
      parcoords = require("parcoords"),
      _ = require("underscore"),
      chroma = require("chroma"),
      Scatter2D = require("Scatter2D"),
      Histogram = require("Histogram");

    var divId = "table-" + this._content.id;
    var config = this.config;
    var id = this.id;

    var table = tableAndTypes.rows;
    var types = {};
    for (columnName in tableAndTypes.types) {
      types[columnName + " "] = tableAndTypes.types[columnName];
    }

    var numColumns = Object.keys(table[0])
      .length;
    var shouldDrawParcoords = numColumns > 2;
    var shouldDrawScatter = numColumns == 2;

    // slickgrid needs each data element to have an id
    var ids = table.map(function(row, i) {
      return {
        id: i
      };
    });
    var data = ids.map(function(idObject, i) {
      var dataObject = {
        id: idObject.id
      }
      var tableRow = table[i];
      for (key in tableRow) {
        var keyWithIdReplaced = (key == "id") ? "_id" : key;
        dataObject[keyWithIdReplaced + " "] = tableRow[key];
      }
      return dataObject
    });

    var parcoords = {};
    var parcoordsDivId = divId + "-pcvis";
    if (shouldDrawParcoords) {
      this._parcoordsDiv = document.createElement('div');
      this._parcoordsDiv.setAttribute("id", parcoordsDivId);
      this._parcoordsDiv.setAttribute("class", 'parcoords');
      this._parcoordsDiv.style.height = this._height / 5 * 2
      this._content.appendChild(this._parcoordsDiv);
      this._hideLabelButton = document.createElement('div');
      this._header.appendChild(this._hideLabelButton);
      var parcoords = d3.parcoords()("#" + parcoordsDivId)
        .data(data)
        .dimensions(Object.keys(types))
        .types(types)
        .color("#1f77b4")
        .alpha(0.3)
        .margin({
          top: this._margin.top,
          left: this._margin.left,
          right: this._margin.right,
          bottom: 10
        })
        .width(this._width)
        .height(this._height / 5 * 2)
        .mode("queue")
        .rate(60)
        .hideAxis(["id"])
        .render()
        .reorderable()
        .brushMode("1D-axes")
        .interactive();

      // Define a gradient for the color selector
      var gradient = d3.selectAll("svg")
        .append("svg:defs")
        .append("svg:linearGradient")
        .attr("id", "gradient")
        .attr(
          "x1", "0%")
        .attr("y1", "0%")
        .attr("x2", "100%")
        .attr("y2", "0%")
        .attr("spreadMethod", "pad");
      gradient.append("svg:stop")
        .attr("offset", "0%")
        .attr("stop-color", "#ffff00")
        .attr("stop-opacity", 1);
      gradient.append("svg:stop")
        .attr("offset", "100%")
        .attr("stop-color", "#ff5500")
        .attr("stop-opacity", 1);

      function colorDimension(dimensions, dimension) {
        dimensions.filter(function(d) {
            return d == dimension;
          })
          .selectAll("circle")
          .attr("class", "colorSelector-selected");
        var values = data.map(function(row) {
          return row[dimension];
        });
        var scale;
        if (types[dimension] == "string") {
          var uniqValues = _.uniq(values)
            .reduce(function(uniqIndexes, value, index) {
              uniqIndexes[value] = index;
              return uniqIndexes;
            }, {});
          var domain = [0, Object.keys(uniqValues)
            .length - 1
          ];
          var chromaScale = chroma.scale('Set1')
            .domain(domain);
          scale = function(v) {
            return chromaScale(uniqValues[v]);
          };
        } else {
          var domain = [Math.min.apply(null, values), Math.max.apply(null, values)];
          var chromaScale = chroma.scale(['orange', 'maroon'])
            .domain(domain);
          scale = function(v) {
            return chromaScale(v);
          };
        }

        parcoords.color(function(d) {
            // color depending on selected dimension
            var value = d[dimension];
            return scale(value);
          })
          .render();
      }

      function changeColor(dimension) {
        var dimensions = parcoords.svg.selectAll(".dimension");
        dimensions.selectAll("circle")
          .attr("class", "colorSelector");
        if (config.coloredDimension != dimension) {
          config.coloredDimension = dimension;
          colorDimension(dimensions, dimension);
        } else {
          parcoords.color("#1f77b4");
          config.coloredDimension = null;
        }
      }

      // click circle to activate coloring
      parcoords.svg.selectAll(".dimension")
        .selectAll(".axis")
        .append("circle")
        .attr("r", 4)
        .attr("class",
          "colorSelector")
        .attr("transform", "translate(0,-25)")
        .attr("text-anchor", "middle")
        .on("click", changeColor)
        .append("svg:title")
        .text("Color data based on this dimension");

      if (config.coloredDimension != null) {
        colorDimension(parcoords.svg.selectAll(".dimension"), config.coloredDimension);
      }

      var labels = parcoords.svg.selectAll(".tick")
        .selectAll("text");
      var button = this._hideLabelButton;
      button.onclick = function() {
        if (config.tickLabelsHidden) {
          labels.attr("visibility", "visible");
          button.setAttribute("class", "hideLabelButton headerButton unhidden");
          button.setAttribute("title", "Hide Ticks Labels");
          config.tickLabelsHidden = false;
        } else {
          labels.attr("visibility", "hidden");
          button.setAttribute("class", "hideLabelButton headerButton hidden");
          button.setAttribute("title", "Show Ticks Labels");
          config.tickLabelsHidden = true;
        }
      };
      if (!config.tickLabelsHidden) {
        labels.attr("visibility", "visible");
        button.setAttribute("class", "hideLabelButton headerButton unhidden");
        button.setAttribute("title", "Hide Ticks Labels");
      } else {
        labels.attr("visibility", "hidden");
        button.setAttribute("class", "hideLabelButton headerButton hidden");
        button.setAttribute("title", "Show Ticks Labels");
      }
    } else if (shouldDrawScatter) {
      var scatterPoints = table.map(function(row) {
        columnKeys = Object.keys(row);
        return {
          x: row[columnKeys[0]],
          y: row[columnKeys[1]]
        };
      });
      var scatterTypes = {
        x: types[Object.keys(types)[0]],
        y: types[Object.keys(types)[1]]
      };
      var scatterDivId = divId + "-scatter"
      this._graphDiv = Util.generateDiv(this._content, scatterDivId);
      // TODO Visualizations stateless and button handling moved to UI part? See comment in #237.
      this._scatter = new Scatter2D(id)
        .header(this._header.id)
        .content(scatterDivId)
        .width(this._width)
        .height(this._height / 5 * 2)
        .margin({
          top: 15,
          right: 30,
          bottom: 30,
          left: 50
        })
        .data({
          points: scatterPoints,
          types: scatterTypes
        })
        .draw();
    } else {
      var singleColumn = table.map(function(row) {
        return row[Object.keys(row)[0]];
      });
      if (types[Object.keys(types)[0]] == "number" && singleColumn.length > 1) {
        var bins = d3.layout.histogram()
          .bins(100)(singleColumn);
        bins = bins.map(function(bin) {
          return {
            start: bin.x,
            end: bin.x + bin.dx,
            y: bin.y
          };
        });
        var hist = new Histogram()
          .header(this._header.id)
          .content(this._content.id)
          .margin({
            top: 15,
            right: 30,
            bottom: 30,
            left: 50
          })
          .width(this._width)
          .height(this._height / 5 * 2)
          .data(bins)
          .draw();
      } else {
        var chartId = divId + "-chart";
        Util.generateDiv(this._content, chartId);
        var singleColumnCounts = singleColumn.reduce(function(counts, value) {
          counts[value] = counts[value] ? counts[value] + 1 : 1;
          return counts;
        }, {});
        var singleColumnCountsForC3 = Object.keys(singleColumnCounts)
          .map(function(key) {
            return {
              value: key,
              count: singleColumnCounts[key]
            };
          });
        var chart = {
          bindto: "#" + chartId,
          data: {
            json: singleColumnCountsForC3,
            keys: {
              x: "value",
              value: ["count"]
            },
            type: "bar"
          },
          axis: {
            x: {
              type: "category"
            }
          }
        };
        chart.size = {
          width: this._width,
          height: this._height / 5 * 2
        };
        chart.padding = {
          right: 15,
          top: 10
        };
        c3.generate(chart);
      }
    }

    var pagerId = divId + "-pager";
    var pager = document.createElement('div');
    pager.setAttribute("id", pagerId);
    this._content.appendChild(pager);
    var gridId = divId + "-grid";
    var grid = document.createElement('div');
    grid.setAttribute("id", gridId);
    grid.style.height = this._height / 5 * 3 - 47
    this._content.appendChild(grid);

    // setting up grid
    var column_keys = d3.keys(data[0]);
    var columns = column_keys.map(function(key, i) {
      var column = {
        id: key,
        name: key,
        field: key,
        sortable: true
      };
      if (key == "id") {
        column.width = 40;
      }
      return column;
    });

    var options = {
      enableCellNavigation: true,
      enableColumnReorder: false,
      multiColumnSort: false,
      defaultColumnWidth: 140
    };

    var dataView = new Slick.Data.DataView();
    var grid = new Slick.Grid("#" + gridId, dataView, columns, options);
    var pager = new Slick.Controls.Pager(dataView, grid, $("#" + pagerId));

    // wire up model events to drive the grid
    dataView.onRowCountChanged.subscribe(function(e, args) {
      grid.updateRowCount();
      grid.render();
    });

    dataView.onRowsChanged.subscribe(function(e, args) {
      grid.invalidateRows(args.rows);
      grid.render();
    });

    // column sorting
    var sortcol = column_keys[0];
    var sortdir = 1;

    function comparer(a, b) {
      var x = a[sortcol],
        y = b[sortcol];
      return (x == y ? 0 : (x > y ? 1 : -1));
    }

    // click header to sort grid column
    grid.onSort.subscribe(function(e, args) {
      sortdir = args.sortAsc ? 1 : -1;
      sortcol = args.sortCol.field;

      if ($.browser.msie && $.broswer.version <= 8) {
        dataView.fastSort(sortcol, args.sortAsc);
      } else {
        dataView.sort(comparer, args.sortAsc);
      }
    });

    if (shouldDrawParcoords) {
      // highlight row in chart
      grid.onMouseEnter.subscribe(function(e, args) {
        var i = grid.getCellFromEvent(e)
          .row;
        var d = parcoords.brushed() || data;
        parcoords.highlight([d[i]]);
      });
      grid.onMouseLeave.subscribe(function(e, args) {
        parcoords.unhighlight();
      });
      // update grid on brush
      parcoords.on("brush", function(d) {
        gridUpdate(d);
      });
    }

    // fill grid with data
    gridUpdate(data);

    function gridUpdate(data) {
      dataView.beginUpdate();
      dataView.setItems(data);
      dataView.endUpdate();
    };
  }

  Table.prototype._clear = function() {
    Util.removeElementIfExists(this._hideLabelButton);
    if (this._scatter != null) {
      this._scatter.clear();
    }
  }

  return Table;

});
