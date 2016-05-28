const ProtoBuf = require('protobufjs');
const mongoose = require('mongoose');

const Schema = mongoose.Schema;
const ObjectId = Schema.Types.ObjectId;

// This is recursive
function schemaFromMessage(TMessage, builder, completedSchemas) {

  if (completedSchemas.has(TMessage)) {
    return completedSchemas.get(TMessage);
  }

  var obj = {};
  var fields = TMessage.getChildren(ProtoBuf.Reflect.Message.Field);
  fields.forEach(function(field) {
    var val = {};
    var typeName = field.type.name === 'message' ? field.resolvedType.name : field.type.name;
    var type = typeFromProto(typeName);

    if (!type) {
      // must reference a different message. Go and build that out
      let typemsg = field.resolvedType;
      if (!typemsg) {
        throw new Error('Can\'t find the type ' + typeName);
      }

      type = schemaFromMessage(typemsg, builder, completedSchemas);
    }

    if (field.options.hasOwnProperty('(objectId)')) {
      type = ObjectId;

      var objIdRef = field.options['(objectId)'];
      if (objIdRef) {
        val.ref = objIdRef;
      }
    }

    if (field.options['(unique)']) {
      val.unique = true;
    }

    val.type = type;

    if (field.required || field.options['(required)']) {
      val.required = true;
    }

    if (field.repeated) {
      val = [val];
    }

    obj[field.name] = val;
  });

  completedSchemas.set(TMessage, obj);
  return obj;
}

function schemaFromProtoSync(fname, messageName) {

  const builder = ProtoBuf.loadProtoFile(fname);
  var TObj = builder.lookup(messageName);

  // Two phase approach. Go and sort out all the schemas where interdependencies are referenced
  // by strings. Then go and substitute the strings for the actual schema variables
  var completedSchemas = new Map(); // Maps message to schema

  return schemaFromMessage(TObj, builder, completedSchemas);
}

function typeFromProto(type) {
  switch (type) {
    case 'bool':
      return Boolean;

    case 'string':
    case 'bytes':
    case 'int64':
    case 'fixed64':
    case 'uint64':
    case 'Duration':
      return String;

    case 'int32':
    case 'fixed32':
    case 'uint32':
    case 'float':
    case 'double':
      return Number;

    case 'Any':
    case 'Struct':
      return Object;

    case 'Timestamp':
      return Date;
  }
}

module.exports = schemaFromProtoSync;
