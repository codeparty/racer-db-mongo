exports.fixId = fixId;

function fixId (doc) {
  if (!doc._id) return;
  doc.id = doc._id.toString();
  delete doc._id;
}
