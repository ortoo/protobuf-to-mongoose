'use strict';

const ProtoBuf = require('protobufjs');
const mongoose = require('mongoose');
const isEmpty = require('lodash.isempty');
const debug = require('debug')('@ortoo/protobuf-to-mongoose');

const Schema = mongoose.Schema;
const ObjectId = Schema.Types.ObjectId;

module.exports = schemaFromProtoSync;

function schemaFromProtoSync(fname, messageName) {

  debug('Generating schema from', fname);

  const builder = ProtoBuf.loadProtoFile(fname);

  if (messageName) {
    return createSchema(messageName);
  } else {
    return createSchema;
  }

  function createSchema(messageName) {
    var TObj = builder.lookup(messageName);

    var completedSchemas = new Map(); // Maps message to schema
    var oneOfRefs = [];
    var validators = [];

    var schema = new Schema(schemaFromMessage(TObj, ''));

    // Add in any virtuals
    for (let middleware of oneOfRefs) {
      schema.pre('save', middleware);
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

        // Ignore virtuals and _id fields - mongoose will add those automagically
        if (field.options['(virtual)'] || field.name === '_id') {
          return;
        }

        var val = {};
        var typeName = field.type.name === 'message' ? field.resolvedType.name : field.type.name;
        var repeated = field.repeated;
        var resolvedType = field.resolvedType;
        var isMap = field.map;

        // We handle wrappers (i.e. messages that wrap a value to allow us to detect their
        // presence on the wire or not)
        var wrapperMatch = typeName.match(/^(\w+)(Array|Value|Map)$/);

        if (wrapperMatch) {
          let valField = field.resolvedType.getChild('value');
          typeName = valField.type.name === 'message' ? valField.resolvedType.name : valField.type.name;
          repeated = valField.repeated;
          resolvedType = valField.resolvedType;
          isMap = valField.map;
        }

        var type = isMap ? Object : typeFromProto(typeName);

        if (!type) {
          // must reference a different message. Go and build that out
          if (!resolvedType) {
            throw new Error('Can\'t find the type ' + typeName);
          }

          type = schemaFromMessage(resolvedType, `${prefix}${field.name}.`);

          // The value is the type here
          val = type;
        } else {
          if (field.options['(oneOfReference)']) {
            let oneof = TMessage.getChild(field.options['(oneOfReference)']);
            let oneOfPaths = oneof.fields.map((field) => `${prefix}${field.name}`);
            oneOfRefs.push(constructOneOfMiddleware(`${prefix}${field.name}`, oneOfPaths));
          }

          if (typeName === 'enum') {
            var enumVals = resolvedType.children.map(child => child.name);
            val.enum = enumVals;
          }

          if (field.options.hasOwnProperty('(objectId)')) {
            type = ObjectId;

            var objIdRef = field.options['(objectId)'];
            if (objIdRef) {
              val.ref = objIdRef;
            }
          }

          ['lowercase', 'uppercase', 'trim', 'min', 'max'].forEach(function (opt) {
            if (field.options[`(${opt})`]) {
              val[opt] = true;
            }
          });

          val.type = type;

          if (field.required || field.options['(required)']) {
            val.required = true;
          }
        }

        if (repeated) {
          val = {type: [val], default: void 0};
        }

        obj[field.name] = val;
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
}

function typeFromProto(type) {

  switch (type) {
    case 'bool':
      return Boolean;

    case 'string':
    case 'bytes':
    case 'int64':
    case 'sint64':
    case 'fixed64':
    case 'sfixed64':
    case 'uint64':
    case 'enum':
    case 'Duration':
      return String;

    case 'int32':
    case 'sint32':
    case 'fixed32':
    case 'sfixed32':
    case 'uint32':
    case 'float':
    case 'double':
      return Number;

    case 'Any':
    case 'Struct':
    case 'JSONObject':
      return Object;

    case 'Timestamp':
      return Date;

    case 'ObjectId':
      return ObjectId;
  }
}

function constructOneOfMiddleware(oneofName, oneofPaths) {
  return function(next) {
    var pathInUse = oneofPaths.find((path) => this.get(path) && !isEmpty(this.get(path)));

    // Return the final part of the path
    var sep = pathInUse && pathInUse.split('.');
    this.set(oneofName, sep && sep[sep.length - 1]);
    next();
  };
}

function constructOneOfValidator(oneofName, paths) {
  return function(next) {
    // Check that only one of the paths is set
    var setPaths = paths.filter((path) => this.isInit(path) && this.get(path) && !isEmpty(this.get(path)));
    if (setPaths.length > 1) {
      next(new Error(`Can only set one of the ${oneofName} paths. The following are set: ${setPaths.join(', ')}.`));
    } else {
      next();
    }
  };
}
