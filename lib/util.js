exports.fixId = fixId;

function fixId (doc) {
  if (!doc._id) return;
  // Note that eagerly casting the id to a string sometimes
  // substantially increases performance in later server-side code
  doc.id = doc._id.toString();
  delete doc._id;
}
