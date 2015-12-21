define(function(require) {

  var _ = require("underscore");

  function Util() {};

  Util.prototype.doAndRedoOnResizeOf = function(w, f) {
    f();
    w.onresize = f;
  };

  Util.prototype.flatMap = function(seq, f) {
    return _.flatten(seq.map(f), true);
  };

  Util.prototype.removeElementIfExists = function(element) {
    if (element != null) {
      var parent = element.parentNode;
      if (parent == null) {
        console.warn("Trying to remove " + element.tagName + " (" + element.id +
          ") but parent node does not exist anymore.");
      } else {
        parent.removeChild(element);
      }
    }
  };

  Util.prototype.removeElementByIdIfExists = function(elementId) {
    this.removeElementIfExists(document.getElementById(elementId));
  };

  Util.prototype.generateElement = function(root, id, type) {
    var element = document.createElement(type);
    element.setAttribute("id", id);
    root.appendChild(element);
    return element;
  }

  Util.prototype.generateDiv = function(root, id) {
    return this.generateElement(root, id, "div");
  }

  Util.prototype.generateSpan = function(root, id) {
    return this.generateElement(root, id, "span");
  }

  Util.prototype.generateTextInput = function(root, id) {
    var input = this.generateElement(root, id, "input");
    input.type = "text";
    return input;
  }

  return new Util();

});
