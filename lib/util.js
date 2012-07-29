exports.fixId = fixId;

function fixId (doc) {
  doc.id = doc._id;
  delete doc._id;
}
