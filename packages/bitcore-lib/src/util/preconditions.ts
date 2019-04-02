import * as _ from 'lodash';
import BitcoreError from '../errors';

export default {
  checkState(condition, message) {
    if (!condition) {
      throw new BitcoreError('InvalidState', message);
    }
  },
  checkArgument(condition, argumentName?, message?, docsPath?) {
    if (!condition) {
      throw new BitcoreError(
        'InvalidArgument',
        argumentName,
        message,
        docsPath
      );
    }
  },
  checkArgumentType(argument, type, argumentName = '(unknown name)') {
    if (_.isString(type)) {
      if (type === 'Buffer') {
        const buffer = require('buffer'); // './buffer' fails on cordova & RN
        if (!buffer.Buffer.isBuffer(argument)) {
          throw new BitcoreError(
            'InvalidArgumentType',
            argument,
            type,
            argumentName
          );
        }
      } else if (typeof argument !== type) {
        throw new BitcoreError(
          'InvalidArgumentType',
          argument,
          type,
          argumentName
        );
      }
    } else {
      if (!(argument instanceof type)) {
        throw new BitcoreError(
          'InvalidArgumentType',
          argument,
          type.name,
          argumentName
        );
      }
    }
  }
};
