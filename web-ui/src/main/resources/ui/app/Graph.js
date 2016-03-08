define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util"),
    Cache = require("Cache");

  function Graph(id) {
    if (Cache.existsConfig(id)) {
      this.config = Cache.getConfig(id);
    } else {
      this.config = {
        drawNodeLabels: true,
        drawEdgeLabels: true,
        drawDirections: true
      };
      Cache.setConfig(id, this.config);
    }
  }

  Graph.prototype = new Visualization();
  Graph.prototype.constructor = Visualization;
  Graph.prototype.parent = Visualization.prototype;

  Graph.prototype._draw = function(graph) {
    var dds = require("dds");
    var d3 = require("d3");
    var config = this.config;
    var nodeButton = document.createElement('div');
    var edgeButton = document.createElement('div');
    var directionButton = document.createElement('div');
    var instance = this;

    function drawGraph() {
        if (config.drawNodeLabels) {
            nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton visible");
            nodeButton.setAttribute("title", "Hide node labels");
        } else {
            nodeButton.setAttribute("class", "triggerNodeLabelsButton headerButton hidden");
            nodeButton.setAttribute("title", "Draw node labels");
        }

        if (config.drawEdgeLabels) {
            edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton visible");
            edgeButton.setAttribute("title", "Hide edge labels");
        } else {
            edgeButton.setAttribute("class", "triggerEdgeLabelsButton headerButton hidden");
            edgeButton.setAttribute("title", "Draw edge labels");
        }

        if (config.drawDirections) {
            directionButton.setAttribute("class", "triggerDirectionsButton headerButton visible");
            directionButton.setAttribute("title", "Draw undirected edges");
        } else {
            directionButton.setAttribute("class", "triggerDirectionsButton headerButton hidden");
            directionButton.setAttribute("title", "Draw directed edges");
        }
    }

    nodeButton.onclick = function() {
      config.drawNodeLabels = !config.drawNodeLabels;
      drawGraph();
    }
    this._header.appendChild(nodeButton);
    this._triggerNodeLabelsButton = nodeButton;

    edgeButton.onclick = function() {
      config.drawEdgeLabels = !config.drawEdgeLabels;
      drawGraph();
    }
    this._header.appendChild(edgeButton);
    this._triggerEdgeLabelsButton = edgeButton;

    directionButton.setAttribute("id", "triggerDirectionsButton");
    directionButton.onclick = function() {
      config.drawDirections = !config.drawDirections;
      drawGraph();
    }
    this._header.appendChild(directionButton);
    this._triggerDirectionsButton = directionButton;

    drawGraph();
  }

  Graph.prototype._clear = function() {
    Util.removeElementIfExists(this._triggerNodeLabelsButton);
    Util.removeElementIfExists(this._triggerEdgeLabelsButton);
    Util.removeElementIfExists(this._triggerDirectionsButton);
  }

  return Graph;

});
