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

  // TODO at least stringify map type
  // TODO byte array with proper toString
  // TODO complex schema does not work (struct, array, etc.)
  Table.prototype._draw = function(tableAndTypes) {
    var dds = require("dds");
    //
    tableAndTypes.schema = tableAndTypes.schema.map(function(dct) {
        dct["type"] = JSON.parse(dct["type"]);
        return dct;
    });
    this._chartDiv = dds.table(tableAndTypes.title, tableAndTypes.schema, tableAndTypes.content);
    this._content.appendChild(this._chartDiv);
  }

  Table.prototype._clear = function() {
    Util.removeElementIfExists(this._hideLabelButton);
  }

  return Table;

});
