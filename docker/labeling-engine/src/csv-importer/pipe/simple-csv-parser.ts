import { Transform } from 'stream';
import * as Papa from 'papaparse';

export class CSVParser extends Transform {
  line: string;
  givenHeaders: string[];
  parsedHeaders: string[];

  constructor(headers: string[], options = {}) {
    super({ ...options, objectMode: true });

    this.givenHeaders = headers;
    this.line = '';
    this.parsedHeaders = null;
  }

  _transform(chunk, encoding, callback) {
    const chunkString = chunk.toString();
    const lines = chunkString.split('\n');
    const regex = /"[^"]*"|[^",]+|(?<=^|,)(?=,|$)/g;

    lines[0] = this.line + lines[0];
    this.line = lines.pop();
    lines.forEach((line) => {
      const values = line
        .match(regex)
        .map((item) => item.trim().replace(/^"(.*)"$/, '$1'));

      if (!this.parsedHeaders) {
        this.parsedHeaders = values;
        this.givenHeaders.every((v, i) => v === this.parsedHeaders[i]);
        return;
      }

      const record = {};
      this.parsedHeaders.forEach((header, index) => {
        record[header] = values[index];
      });

      this.push(record);
    });

    callback();
  }

  _flush(callback) {
    if (this.line) {
      const values = this.line.split(',');

      if (!this.parsedHeaders) {
        this.parsedHeaders = values;
      } else {
        const record = {};
        this.parsedHeaders.forEach((header, index) => {
          record[header] = values[index];
        });

        this.push(record);
      }
    }

    callback();
  }
}
