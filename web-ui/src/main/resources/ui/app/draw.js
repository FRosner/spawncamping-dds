define(function(require) {

  function drawServable(servableIdAndType, headerId, contentId) {
    var Visualization = require("Visualization"),
      C3Chart = require("C3Chart"),
      Empty = require("Empty"),
      Composite = require("Composite"),
      Graph = require("Graph"),
      Histogram = require("Histogram"),
      KeyValueSequence = require("KeyValueSequence"),
      Matrix = require("Matrix"),
      Scatter2D = require("Scatter2D"),
      Table = require("Table");

    var toDraw;
    var id = servableIdAndType.id;
    var servable = servableIdAndType.servable;
    var servableType = servableIdAndType.type;

    if (servableType == "composite") {
      toDraw = new Composite(id)
        .margin({
          top: 30,
          right: 0,
          bottom: 0,
          left: 0
        })
        .data(servable);
    } else if (servableType == "blank") {
      toDraw = new Empty();
    } else if (servableType == "keyValueSequence") {
      toDraw = new KeyValueSequence()
        .data(servable);
    } else if (servableType == "bar") {
      var c3Chart = {
        data : {
          columns : servable.series.map(function(label, idx) {
            return [label].concat(servable.heights[idx]);
          }),
          types : servable.series.reduce(function(agg, label) {
            agg[label] = "bar";
            return agg;
          }, {})
        },
        axis : {
          x : {
            type : "category",
            categories : servable.xDomain
          }
        }
      }
      servable.c3 = c3Chart;
      toDraw = new C3Chart()
        .margin({
          top: 5,
          right: 15,
          left: 60
        })
        .data(servable);
    } else if (servableType == "pie") {
      var c3Chart = {
        data : {
          columns : servable.categoryCountPairs.map(function(categoryCountPair) {
            var category = categoryCountPair[0];
            var count = categoryCountPair[1];
            return [category, count];
          }),
          type : "pie"
        }
      }
      servable.c3 = c3Chart;
      toDraw = new C3Chart()
        .margin({
          top: 5,
          right: 15,
          left: 60
        })
        .data(servable);
    } else if (servableType == "table") {
      toDraw = new Table(id)
        .margin({
          top: 30,
          right: 0,
          bottom: 0,
          left: 0
        })
        .data(servable);
    } else if (servableType == "histogram") {
      toDraw = new Histogram()
        .margin({
          top: 20,
          right: 60,
          bottom: 60,
          left: 60
        })
        .data(servable);
    } else if (servableType == "graph") {
      toDraw = new Graph(id)
        .data(servable);
    } else if (servableType == "scatter") {
      toDraw = new Scatter2D(id)
        .margin({
          top: 10,
          right: 15,
          bottom: 60,
          left: 60
        })
        .data(servable);
    } else if (servableType == "heatmap") {
      toDraw = new Matrix(id)
        .margin({
          top: 10,
          right: 15,
          bottom: 60,
          left: 60
        })
        .data(servable);
    } else {
      console.error("Unrecognized response: " + response);
    }
    if (toDraw != null) {
      toDraw = toDraw.header(headerId)
        .content(contentId)
        .title(servable.title)
        .draw();
      return toDraw;
    }
  }

  return {
    drawServable : drawServable
  };

});
