'use strict';

const ProtoBuf = require('protobufjs');
const mongoose = require('mongoose');
const isEmpty = require('lodash.isempty');
const debug = require('debug')('@ortoo/protobuf-to-mongoose');

const Schema = mongoose.Schema;
const ObjectId = Schema.Types.ObjectId;

const COLLATION_OPTIONS = {
  locale: 'en',
  caseLevel: false,
  strength: 1,
};

module.exports = schemaFromProtoSync;
module.exports.COLLATION_OPTIONS = COLLATION_OPTIONS;

const maxDate = new Date('2099-12-31T23:59:59.999Z');
const minDate = new Date('1800-01-01T00:00:00.000Z');
const sensibleDateValidator = {
  message:
    'validation of `{PATH}` failed with value `{VALUE}` - the date is either waaay in the future or waaay in the past and therefore almost certainly incorrect',
  validator: (val) => {
    return val <= maxDate && val >= minDate;
  },
};

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

    var schema = new Schema(schemaFromMessage(TObj, ''), {
      id: false,
      collation: COLLATION_OPTIONS,
    });

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
    function schemaFromMessage(TMessage, prefix, parentRepeated) {
      if (completedSchemas.has(TMessage)) {
        return completedSchemas.get(TMessage);
      }

      var obj = {};
      var fields = TMessage.getChildren(ProtoBuf.Reflect.Message.Field);
      fields.forEach(function (field) {
        // Ignore virtuals  - mongoose will add those automagically
        if (
          field.options['(virtual)'] ||
          (field.name === '_id' && (!prefix || parentRepeated))
        ) {
          return;
        }

        var val = {};
        var typeName =
          field.type.name === 'message'
            ? field.resolvedType.name
            : field.type.name;
        var repeated = field.repeated;
        var resolvedType = field.resolvedType;
        var isMap = field.map;

        // We handle wrappers (i.e. messages that wrap a value to allow us to detect their
        // presence on the wire or not)
        var wrapperMatch = typeName.match(/^(\w+)(Array|Value|Map)$/);

        if (wrapperMatch) {
          let valField = field.resolvedType.getChild('value');
          typeName =
            valField.type.name === 'message'
              ? valField.resolvedType.name
              : valField.type.name;
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

          type = schemaFromMessage(
            resolvedType,
            `${prefix}${field.name}.`,
            repeated
          );

          // The value is the type here
          val = repeated ? new Schema(type, { id: false }) : type;
        } else {
          if (typeName === 'enum') {
            var enumVals = resolvedType.children.map((child) => child.name);
            val.enum = enumVals;
          }

          if (
            Object.prototype.hasOwnProperty.call(field.options, '(objectId)')
          ) {
            type = ObjectId;

            var objIdRef = field.options['(objectId)'];
            if (objIdRef) {
              val.ref = objIdRef;
            }
          }

          ['lowercase', 'uppercase', 'trim', 'min', 'max'].forEach(function (
            opt
          ) {
            if (field.options[`(${opt})`]) {
              val[opt] = true;
            }
          });

          if (type === Date) {
            val.validate = sensibleDateValidator;
          }

          val.type = type;

          if (field.required || field.options['(required)']) {
            val.required = true;
          }
        }

        if (repeated) {
          val = { type: [val] /* , default: void 0 */ };
        }

        obj[field.name] = val;
      });

      // Add any oneof fields and validators
      var oneofs = TMessage.getChildren(ProtoBuf.Reflect.Message.OneOf);
      oneofs.forEach(function (oneof) {
        obj[oneof.name] = String;
        var oneofPaths = oneof.fields.map((field) => `${prefix}${field.name}`);
        oneOfRefs.push(
          constructOneOfMiddleware(prefix, oneof.name, oneofPaths)
        );
        validators.push(
          constructOneOfValidator(`${prefix}${oneof.name}`, oneofPaths)
        );
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
    case 'HybridObject':
    case 'BSONObject':
      return Object;

    case 'Timestamp':
      return Date;

    case 'ObjectId':
      return ObjectId;
  }
}

function constructOneOfMiddleware(prefix, oneofName, oneofPaths) {
  return function (next) {
    var pathInUse = this.get(`${prefix}${oneofName}`);

    if (pathInUse) {
      oneofPaths.forEach((path) => {
        if (`${prefix}${pathInUse}` !== path) {
          this.set(path, undefined);
        }
      });
    }
    next();
  };
}

function constructOneOfValidator(oneofName, paths) {
  return function (next) {
    // Check that only one of the paths is set
    var setPaths = paths.filter(
      (path) => this.isInit(path) && this.get(path) && !isEmpty(this.get(path))
    );
    if (setPaths.length > 1) {
      next(
        new Error(
          `Can only set one of the ${oneofName} paths. The following are set: ${setPaths.join(
            ', '
          )}.`
        )
      );
    } else {
      next();
    }
  };
}
