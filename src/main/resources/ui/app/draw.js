define(function(require) {

  function drawServable(servable, headerId, contentId) {
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
    var id = servable.id;
    if (servable.type == "composite") {
      toDraw = new Composite(id)
        .margin({
          top: 30,
          right: 0,
          bottom: 0,
          left: 0
        })
        .data(servable.content);
    } else if (servable.type == "empty") {
      toDraw = new Empty();
    } else if (servable.type == "keyValue") {
      toDraw = new KeyValueSequence()
        .data(servable.content);
    } else if (servable.type == "bar (indexed)" || servable.type == "bar (categorical)" || servable.type == "pie" || servable.type == "line") {
      toDraw = new C3Chart()
        .margin({
          top: 5,
          right: 15,
          left: 60
        })
        .data(servable.content);
    } else if (servable.type == "table") {
      toDraw = new Table(id)
        .margin({
          top: 30,
          right: 0,
          bottom: 0,
          left: 0
        })
        .data(servable.content);
    } else if (servable.type == "histogram") {
      toDraw = new Histogram()
        .margin({
          top: 20,
          right: 60,
          bottom: 60,
          left: 60
        })
        .data(servable.content);
    } else if (servable.type == "graph") {
      toDraw = new Graph(id)
        .data(servable.content);
    } else if (servable.type == "points-2d") {
      toDraw = new Scatter2D(id)
        .margin({
          top: 10,
          right: 15,
          bottom: 60,
          left: 60
        })
        .data(servable.content);
    } else if (servable.type == "matrix") {
      toDraw = new Matrix(id)
        .margin({
          top: 10,
          right: 15,
          bottom: 60,
          left: 60
        })
        .data(servable.content);
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
