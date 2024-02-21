jest.mock('@google-cloud/storage');

const { Storage } = require('@google-cloud/storage');
const iconv = require('iconv-lite');
const { convertFileEncoding } = require('./index');

describe('convertFileEncoding', () => {
    let mockCreateReadStream;
    let mockCreateWriteStream;

    beforeEach(() => {
        jest.clearAllMocks();

        const mockReadStream = {
            pipe: jest.fn().mockReturnThis(), // この pipe が2回呼ばれるはず
            on: jest.fn().mockImplementation((event, handler) => {
                if (event === 'finish') {
                    handler();
                } else if (event === 'error') {
                    handler(new Error('An error occurred'));
                }
                return mockReadStream; // ここで mockReadStream を返す
            }),
        };

        mockCreateReadStream = jest.fn(() => mockReadStream);

        const mockWriteStream = {
            on: jest.fn().mockImplementation((event, handler) => {
                if (event === 'finish') {
                    handler();
                } else if (event === 'error') {
                    handler(new Error('An error occurred'));
                }
                return mockWriteStream; // ここで mockWriteStream を返す
            }),
        };

        mockCreateWriteStream = jest.fn(() => mockWriteStream);

        Storage.prototype.bucket.mockImplementation(() => ({
            file: jest.fn(() => ({
                createReadStream: mockCreateReadStream,
                createWriteStream: mockCreateWriteStream,
            })),
        }));
    });

    test('reads data from a bucket, transforms encoding, and writes to a different bucket', async () => {
        const bucketName = 'source-bucket';
        const filePath = 'source-file-path';
        const tempBucket = 'temp-bucket';
        const tempFilePath = 'temp-file-path';

        await convertFileEncoding(bucketName, filePath, tempBucket, tempFilePath);

        expect(Storage.prototype.bucket).toHaveBeenCalledWith(bucketName);
        expect(Storage.prototype.bucket().file().createReadStream).toHaveBeenCalled();
        expect(mockCreateReadStream).toHaveBeenCalled();

        expect(mockCreateReadStream().pipe).toHaveBeenCalledTimes(3); // decodeStream, encodeStream, convertStream

        expect(Storage.prototype.bucket).toHaveBeenCalledWith(tempBucket);
        expect(mockCreateWriteStream).toHaveBeenCalled();
        expect(Storage.prototype.bucket().file().createWriteStream).toHaveBeenCalled();
    });
});
