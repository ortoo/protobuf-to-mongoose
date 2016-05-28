'use strict';

const ProtoBuf = require('protobufjs');
const mongoose = require('mongoose');

const Schema = mongoose.Schema;
const ObjectId = Schema.Types.ObjectId;

module.exports = schemaFromProtoSync;

function schemaFromProtoSync(fname, messageName) {

  const builder = ProtoBuf.loadProtoFile(fname);
  var TObj = builder.lookup(messageName);

  var completedSchemas = new Map(); // Maps message to schema
  var virtuals = {};
  var validators = [];

  var schema = new Schema(schemaFromMessage(TObj, ''));

  // Add in any virtuals
  for (let path in virtuals) {
    schema.virtual(path).get(virtuals[path]);
  }

  // Add in the validators
  for (let validator of validators) {
    schema.pre('validate', validator);
  }

  return schema;

  // This is recursive
  function schemaFromMessage(TMessage, prefix) {

    if (completedSchemas.has(TMessage)) {
      return completedSchemas.get(TMessage);
    }

    var obj = {};
    var fields = TMessage.getChildren(ProtoBuf.Reflect.Message.Field);
    fields.forEach(function(field) {
      var val = {};
      var typeName = field.type.name === 'message' ? field.resolvedType.name : field.type.name;
      var type = typeFromProto(typeName);

      if (field.options['(oneOfReference)']) {
        // We're a reference of a oneOf set. Find out what paths are in the oneOf
        var oneof = TMessage.getChild(field.options['(oneOfReference)']);
        var oneOfPaths = oneof.fields.map((field) => `${prefix}${field.name}`);
        virtuals[`${prefix}${field.name}`] = constructOneOfVirtual(oneOfPaths);
      } else {
        if (!type) {
          // must reference a different message. Go and build that out
          let typemsg = field.resolvedType;
          if (!typemsg) {
            throw new Error('Can\'t find the type ' + typeName);
          }

          type = schemaFromMessage(typemsg, `${prefix}${field.name}.`);

          // The value is the type here
          val = type;
        } else {
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
        }

        if (field.repeated) {
          val = [val];
        }

        obj[field.name] = val;
      }
    });

    // Add any oneof validators
    var oneofs = TMessage.getChildren(ProtoBuf.Reflect.Message.OneOf);
    oneofs.forEach(function(oneof) {
      var oneofPaths = oneof.fields.map((field) => `${prefix}${field.name}`);
      validators.push(constructOneOfValidator(`${prefix}${oneof.name}`, oneofPaths));
    });

    completedSchemas.set(TMessage, obj);
    return obj;
  }
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

function constructOneOfVirtual(oneofPaths) {
  return function() {
    var pathInUse = oneofPaths.find((path) => this.get(path));

    // Return the final part of the path
    var sep = pathInUse.split('.');
    return sep[sep.length - 1];
  };
}

function constructOneOfValidator(oneofName, paths) {
  return function(next) {
    // Check that only one of the paths is set
    var setPaths = paths.filter((path) => !!this.get(path));
    if (setPaths.length > 1) {
      next(new Error(`Can only set one of the ${oneofName} paths. The following are set: ${setPaths.join(', ')}.`));
    } else {
      next();
    }
  };
}
