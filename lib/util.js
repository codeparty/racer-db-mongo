exports.fixId = fixId;
exports.isObjectIdString = isObjectIdString;
exports.objectIdXfSelector = objectIdXfSelector;

function fixId (doc) {
  if (!doc._id) return;
  // Note that eagerly casting the id to a string sometimes
  // substantially increases performance in later server-side code
  doc.id = doc._id.toString();
  delete doc._id;
}

var checkForHexRegExp = new RegExp("^[0-9a-fA-F]{24}$");

function isObjectIdString (id) {
  // Throw an error if it's not a valid setup
  if(id != null && 'number' != typeof id && (id.length != 12 && id.length != 24)) {
    return false;
  }
  return checkForHexRegExp.test(id);
}

function objectIdXfSelector (selector, collection) {
  var id = selector._id;
  if (id && isObjectIdString(id)) {
    selector._id = collection.id(id);
  }
  return selector;
}