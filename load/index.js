// Function to convert file encoding from Shift_JIS to UTF8
const { Storage } = require('@google-cloud/storage');
const iconv = require('iconv-lite');
const storage = new Storage();

async function convertFileEncoding(bucketName, filePath, tempBucket, tempFilePath) {
    const readStream = storage.bucket(bucketName).file(filePath).createReadStream();
    const convertStream = readStream.pipe(iconv.decodeStream('cp932')).pipe(iconv.encodeStream('utf8'));

    const writeStream = storage.bucket(tempBucket).file(tempFilePath).createWriteStream();

    await new Promise((resolve, reject) => {
        convertStream.pipe(writeStream).on('finish', resolve).on('error', reject);
    });
}

// Function to delete tempfile from Cloud Storage
async function deleteTempFile(tempBucket, tempFilePath) {
    await storage.bucket(tempBucket).file(tempFilePath).delete();
}

// Function to load file to BigQuery
const { BigQuery } = require('@google-cloud/bigquery');
const bigquery = new BigQuery();
const fs = require('fs');

async function loadFileToBigQuery(datasetId, tableId, bucketName, filePath, jobConfig) {
    await bigquery.dataset(datasetId).table(tableId).load(storage.bucket(bucketName).file(filePath), jobConfig);
}
const datasetSchemas = {
    'jader': {
        fields: [
            { name: 'ID', type: 'STRING' },
            { name: 'TIMES_REPORTED', type: 'INT64' },
            { name: 'SEX', type: 'STRING' },
            { name: 'AGE', type: 'STRING' },
            { name: 'WEIGHT', type: 'STRING' },
            { name: 'HEIGHT', type: 'STRING' },
            { name: 'FYEAR_QUARTER_REPORTED', type: 'STRING' },
            { name: 'SURVEY_STATUS', type: 'STRING' },
            { name: 'REPORT_CATEGORY', type: 'STRING' },
            { name: 'REPORTER_LICENSE', type: 'STRING' },
            { name: 'E2B', type: 'STRING' }
        ],
    },
    'hist': {
        fields: [
            { name: 'ID', type: 'STRING' },
            { name: 'TIMES_REPORTED', type: 'INT64' },
            { name: 'PRIMARY_DESEASE_CODE', type: 'INT64' },
            { name: 'PRIMARY_DESEASE', type: 'STRING' },
        ],
    },
    'drug': {
        fields: [
            { name: 'ID', type: 'STRING' },
            { name: 'TIMES_REPORTED', type: 'INT64' },
            { name: 'DRUG_CODE', type: 'INT64' },
            { name: 'DRUG_INVOLVEMENT', type: 'STRING' },
            { name: 'DRUG_GENERAL_NAME', type: 'STRING' },
            { name: 'DRUG_PRODUCT_NAME', type: 'STRING' },
            { name: 'ADMINISTRATION_ROUTE', type: 'STRING' },
            { name: 'DATE_ADMINISTRATION_START', type: 'STRING' },
            { name: 'DATE_ADMINISTRATION_END', type: 'STRING' },
            { name: 'AMOUNT_ADMINISTRATION', type: 'INT64' },
            { name: 'UNIT_ADMINISTRATION', type: 'STRING' },
            { name: 'TIMES_DIVIDED_ADMINISTRATION', type: 'INT64' },
            { name: 'REASON_ADMINISTRATION', type: 'STRING' },
            { name: 'DRUG_MANIPURATION', type: 'STRING' },
            { name: 'RECURRENCED_BY_READMINISTRATION', type: 'STRING' },
            { name: 'RISK_CLASS', type: 'STRING' },
        ],
    },
    'reac': {
        fields: [
            { name: 'ID', type: 'STRING' },
            { name: 'TIMES_REPORTED', type: 'INT64' },
            { name: 'ADVERSE_EVENT_CODE', type: 'INT64' },
            { name: 'OUTCOME', type: 'STRING' },
            { name: 'DATE_ADVERSE_EVENT_RECURRENCED', type: 'STRING' },
        ],
    },
};

// Entrypoint
const functions = require('@google-cloud/functions-framework');
const tmp = require('tmp');
const path = require('path');

functions.cloudEvent('uploadCsvToBigQuery', async (cloudEvent) => {
    const file = cloudEvent.data;
    const bucketName = file.bucket;
    const tempBucket = 'jader_temp'; // use another bucket to avoid infinite loop
    const filePath = file.name;
    const fileName = path.basename(filePath);
    const tempFileName = path.basename(tmp.tmpNameSync()) + '.csv';
    const tempFilePath = `temp/${tempFileName}`;

    console.log(`fileName: ${fileName}`);
    console.log(`filePath: ${filePath}`);
    console.log(`tempFileName: ${tempFileName}`);
    console.log(`tempFilePath: ${tempFilePath}`);
    if (!fileName.endsWith('.csv')) {
        console.log('not csv');
        return;
    }

    const datasetId = 'jader';
    const tableId = fileName.replace(/[0-9]{6}\.csv/, '');

    const jobConfig = {
        sourceFormat: 'CSV',
        schema: datasetSchemas[fileName.replace(/[0-9]{6}\.csv/, '')],
        skipLeadingRows: 1,
        autodetect: false,
    };

    try {
        console.log('Converting file encoding...');
        await convertFileEncoding(bucketName, filePath, tempBucket, tempFilePath);

        console.log('Loading file to BigQuery...');
        await loadFileToBigQuery(datasetId, tableId, tempBucket, tempFilePath, jobConfig);

        console.log('Cleaning up...');
        await deleteTempFile(tempBucket, tempFilePath);

        console.log('Process completed successfully.');
    } catch (error) {
        console.error(`Process failed: ${error.message}`);
        await deleteTempFile(tempBucket, tempFilePath); // Ensure temp file is deleted even on error
        throw error;
    }
});

module.exports = {
    convertFileEncoding,
    deleteTempFile,
    loadFileToBigQuery,
};
