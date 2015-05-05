function doAndRedoOnResize(f) {
  f();
  window.onresize = f;
}

function flatMap(seq, f) {
  return _.flatten(seq.map(f), true);
}

function removeElementIfExists(element) {
  if (element != null) {
    var parent = element.parentNode;
    if (parent == null) {
      console.warn("Trying to remove " + element.tagName + " (" + element.id + ") but parent node does not exist anymore.");
    } else {
      parent.removeChild(element);
    }
  }
}

function removeElementByIdIfExists(elementId) {
  removeElementIfExists(document.getElementById(elementId));
}

function generateDiv(root, id) {
  var div = document.createElement('div');
  div.setAttribute("id", id);
  root.appendChild(div);
  return div;
}