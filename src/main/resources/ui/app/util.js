function doAndRedoOnResize(f) {
    f();
    window.onresize = f;
}

function flatMap(seq, f) {
    return _.flatten(seq.map(f), true);
}

function removeElementIfExists(element) {
    if (element != null) {
        element.parentNode.removeChild(element);
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
