import { Transform } from 'stream';

class StringifyJsonStream extends Transform {
  constructor() {
    super({ objectMode: true });
  }

  _transform(item) {
    return JSON.stringify(item) + '\n';
  }
}

export const stringifyJsonStream = new StringifyJsonStream();
