function doAndRedoOnResizeOf(w, f) {
  f();
  w.onresize = f;
}

function flatMap(seq, f) {
  return _.flatten(seq.map(f), true);
}

function removeElementIfExists(element) {
  if (element != null) {
    var parent = element.parentNode;
    if (parent == null) {
      console.warn("Trying to remove " + element.tagName + " (" + element.id +
        ") but parent node does not exist anymore.");
    } else {
      parent.removeChild(element);
    }
  }
}

function removeElementByIdIfExists(elementId) {
  removeElementIfExists(document.getElementById(elementId));
}

function generateElement(root, id, type) {
  var element = document.createElement(type);
  element.setAttribute("id", id);
  root.appendChild(element);
  return element;
}

function generateDiv(root, id) {
  return generateElement(root, id, "div");
}

function generateSpan(root, id) {
  return generateElement(root, id, "span");
}

function generateTextInput(root, id) {
  var input = generateElement(root, id, "input");
  input.type = "text";
  return input;
}
