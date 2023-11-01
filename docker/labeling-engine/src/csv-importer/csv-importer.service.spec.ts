import { Test, TestingModule } from '@nestjs/testing';
import { CsvImporterService } from './csv-importer.service';

describe('CsvImporterService', () => {
  let service: CsvImporterService;

  beforeEach(async () => {
    const module: TestingModule = await Test.createTestingModule({
      providers: [CsvImporterService],
    }).compile();

    service = module.get<CsvImporterService>(CsvImporterService);
  });

  it('should be defined', () => {
    expect(service).toBeDefined();
  });
});
