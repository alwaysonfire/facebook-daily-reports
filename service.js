const axios = require('axios');
const dotenv = require('dotenv');
const Papa = require('papaparse');
const { MongoClient } = require('mongodb');
const path = require('path');
const moment = require('moment');

dotenv.config();

const dbName = 'dailypull';
const caPath = '/home/ubuntu/global-bundle.pem';
let dev = true;
let uri = 'mongodb://localhost:27017';
let options = {};

if (dev) {
  options = {
    dbName,
    tls: true,
    tlsCAFile: path.resolve(caPath),
    replicaSet: 'rs0',
    readPreference: 'secondaryPreferred',
    retryWrites: 'false',
  };

  uri =
    'mongodb://adminuser:Admin123@daily-pull-vlm.cluster-ct3hcedret2a.eu-central-1.docdb.amazonaws.com:27017';
}

const client = new MongoClient(uri, {
  ...options,
});

exports.dailyPullGetStatsMedia = async () => {
  try {
    await client.connect();
    console.log('Connected to MongoDB');
    const yesterday = moment().subtract(1, 'day').format('YYYY-MM-DD');
    const db = client.db(dbName); // Access the specific database
    const collection = db.collection('stats_media_platforms_new'); // Access the specific collection

    const documents = await collection
      .find({ createdAt: yesterday.toString() })
      .limit(2)
      .toArray();

    return documents;
  } catch (e) {
    console.log(e);
  } finally {
    await client.close(); // Close the connection after use
  }
};
