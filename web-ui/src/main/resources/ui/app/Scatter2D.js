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

  Scatter2D.prototype = new Visualization();
  Scatter2D.prototype.constructor = Visualization;
  Scatter2D.prototype.parent = Visualization.prototype;

  Scatter2D.prototype._draw = function(pointsWithTypes) {
    console.log(pointsWithTypes);
    var dds = require("dds");
    var scatterVis = this;
    var config = this.config;

    if(typeof pointsWithTypes.xIsNumeric === 'undefined' || pointsWithTypes.xIsNumeric === null) {
        pointsWithTypes.xIsNumeric = false;
    }

    if(typeof pointsWithTypes.yIsNumeric === 'undefined' || pointsWithTypes.yIsNumeric === null) {
            pointsWithTypes.yIsNumeric = false;
    }

    function drawScatter(){
        scatterVis._chartDiv = dds.scatterplot(pointsWithTypes.title, pointsWithTypes.points, pointsWithTypes.xIsNumeric,
                                               pointsWithTypes.yIsNumeric, scatterVis.config.jitterEnabled);
        scatterVis._content.innerHTML = "";
        scatterVis._content.appendChild(scatterVis._chartDiv);
    }

    var enableJitterButton = document.createElement('div');
    enableJitterButton.setAttribute("id", "enableJitterButton");
    this._header.appendChild(enableJitterButton);
    enableJitterButton.onclick = function() {
      config.jitterEnabled = !config.jitterEnabled;
      if (config.jitterEnabled) {
        enableJitterButton.setAttribute("class", "headerButton disabled");
        enableJitterButton.setAttribute("title", "Enable Jitter");
      } else {
        enableJitterButton.setAttribute("class", "headerButton enabled");
        enableJitterButton.setAttribute("title", "Disable Jitter");
      }
      drawScatter();
    };
    if (config.jitterEnabled) {
      enableJitterButton.setAttribute("class", "headerButton enabled");
      enableJitterButton.setAttribute("title", "Disable Jitter");
    } else {
      enableJitterButton.setAttribute("class", "headerButton disabled");
      enableJitterButton.setAttribute("title", "Enable Jitter");
    }
    this._enableJitterButton = enableJitterButton;
    drawScatter();
  }

  Scatter2D.prototype._clear = function() {
    Util.removeElementIfExists(this._enableJitterButton);
  }

  return Scatter2D;

});
