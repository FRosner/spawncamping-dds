define(function(require) {

  var Visualization = require("Visualization"),
    Util = require("util"),
    Cache = require("Cache");

  function Scatter2D(id) {
    if (Cache.existsConfig(id)) {
      this.config = Cache.getConfig(id);
    } else {
      this.config = {
        jitterEnabled: false
      };
      Cache.setConfig(id, this.config);
    }
  }

  function updateJitterButton(button, jitterEnabled){
      if (jitterEnabled) {
        button.setAttribute("class", "headerButton enabled");
        button.setAttribute("title", "Disable Jitter");
      } else {
        button.setAttribute("class", "headerButton disabled");
        button.setAttribute("title", "Enable Jitter");
      }
  }

  Scatter2D.prototype = new Visualization();
  Scatter2D.prototype.constructor = Visualization;
  Scatter2D.prototype.parent = Visualization.prototype;

  Scatter2D.prototype._draw = function(pointsWithTypes) {
    var dds = require("dds");
    var scatterVis = this;
    var config = this.config;

    if(typeof pointsWithTypes.xIsNumeric === "undefined" || pointsWithTypes.xIsNumeric === null) {
        pointsWithTypes.xIsNumeric = false;
    }

    if(typeof pointsWithTypes.yIsNumeric === "undefined" || pointsWithTypes.yIsNumeric === null) {
        pointsWithTypes.yIsNumeric = false;
    }

    function drawScatter(){
        scatterVis._chartDiv = dds.scatterPlot(pointsWithTypes.title, pointsWithTypes.points, pointsWithTypes.xIsNumeric,
                                               pointsWithTypes.yIsNumeric, scatterVis.config.jitterEnabled);
        scatterVis._content.innerHTML = "";
        scatterVis._content.appendChild(scatterVis._chartDiv);
    }

    var jitterButton = document.createElement('div');
    jitterButton.setAttribute("id", "enableJitterButton");
    this._header.appendChild(jitterButton);
    jitterButton.onclick = function() {
      config.jitterEnabled = !config.jitterEnabled;
      updateJitterButton(jitterButton, config.jitterEnabled);
      drawScatter();
    };

    updateJitterButton(jitterButton, config.jitterEnabled);
    this.jitterButton = jitterButton;
    drawScatter();
  }

  Scatter2D.prototype._clear = function() {
    Util.removeElementIfExists(this.jitterButton);
  }

  return Scatter2D;

});
