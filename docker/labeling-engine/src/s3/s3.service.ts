import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { GetObjectCommand, S3Client } from '@aws-sdk/client-s3';
import { Readable } from 'stream';
import { createWriteStream } from 'fs';

@Injectable()
export class S3Service {
  private s3Client: S3Client;

  constructor(private configService: ConfigService) {
    this.s3Client = new S3Client({
      credentials: {
        accessKeyId: this.configService.get<string>('AWS_ACCESS_KEY_ID'),
        secretAccessKey: this.configService.get<string>(
          'AWS_SECRET_ACCESS_KEY',
        ),
      },
      region: this.configService.get<string>('AWS_REGION'),
    });
  }

  private fileChunkSize = 20 * 1024 * 1024;

  private getRangeAndLength(contentRange: string) {
    const [range, length] = contentRange.split('/');
    const [start, end] = range.split('-');
    return {
      start: parseInt(start),
      end: parseInt(end),
      length: parseInt(length),
    };
  }

  private isComplete({ end, length }) {
    return end === length - 1;
  }

  async downloadAndSaveFile(s3Url: string, outputPath: string): Promise<void> {
    const parsedUrl = this.parseS3Url(s3Url);
    if (!parsedUrl) {
      throw new Error('Invalid S3 URL provided.');
    }

    const writeStream = createWriteStream(outputPath).on('error', (err) => {
      console.error(err);
      throw err;
    });

    let rangeAndLength = { start: -1, end: -1, length: -1 };

    while (!this.isComplete(rangeAndLength)) {
      const { end } = rangeAndLength;
      const nextRange = { start: end + 1, end: end + this.fileChunkSize };

      console.log(`Downloading bytes ${nextRange.start} to ${nextRange.end}`);

      const { ContentRange, Body } = await this.getObjectRange({
        bucket: parsedUrl.bucket,
        key: parsedUrl.key,
        ...nextRange,
      });

      writeStream.write(await Body.transformToByteArray());
      rangeAndLength = this.getRangeAndLength(ContentRange);
    }
  }

  private async getObjectRange({ bucket, key, start, end }): Promise<any> {
    const command = new GetObjectCommand({
      Bucket: bucket,
      Key: key,
      Range: `bytes=${start}-${end}`,
    });

    return this.s3Client.send(command);
  }

  private parseS3Url(s3Url: string): { bucket: string; key: string } | null {
    const match = s3Url.match(/^s3:\/\/([^\/]+)\/(.+)$/);
    if (!match) return null;
    return { bucket: match[1], key: match[2] };
  }
}
