require('dotenv').config();
// var myArgs = require('optimist').argv;
const url = 'mongodb://localhost:27017/';
const dbConfig = { useNewUrlParser: true };
const mongodb = require('mongodb');
const client = mongodb.MongoClient;
const { DEMS } = require('./helpers');

client.connect(url, dbConfig, async (err, mongoClient) => {
  if (err) throw err;
  const db = mongoClient.db('lib_rad');
  console.log('Mongo connected...');

  for (let i = 0; i < DEMS.length; i++) {
    try {
      await getCandidateStats(db, DEMS[i]);
    } catch (err) {
      console.log(err);
      mongoClient.close();
    }
  }

  mongoClient.close();
});

function getAllIds(db, candidate) {
  return new Promise((res, rej) => {
    const doc = db.collection(candidate.name);

    doc.distinct('_id', {}, {}, (err, result) => {
      result.forEach((val, i) => {
        if (i > 800) {
          console.log(val);
        }
      });
    });
  });
}

function getHighestDonation(db, candidate) {
  return new Promise((res, rej) => {
    const doc = db.collection(candidate.name);

    doc
      .aggregate([
        { $unwind: '$contributions' },
        {
          $match: {
            'contributions.entity_type_desc': { $ne: 'INDIVIDUAL' }
          }
        },
        {
          $sort: {
            'contributions.contribution_receipt_amount': -1
          }
        },
        { $limit: 5 }
      ])
      .toArray((err, result) => {
        console.log(result);
        res();
      });
  });
}

function getCandidateStats(db, candidate) {
  return new Promise((res, rej) => {
    const doc = db.collection(candidate.name);
    doc
      .aggregate([
        { $unwind: '$contributions' },
        { $match: { 'contributions.entity_type_desc': 'INDIVIDUAL' } },
        {
          $project: {
            item: 1,
            donation: '$contributions.contribution_receipt_amount',
            tally: {
              $cond: [
                { $gt: ['$contributions.contribution_receipt_amount', 0] },
                1,
                0
              ]
            },
            sqRoot: {
              $cond: [
                { $lt: ['$contributions.contribution_receipt_amount', 0] },
                {
                  $multiply: [
                    -1,
                    {
                      $sqrt: {
                        $abs: '$contributions.contribution_receipt_amount'
                      }
                    }
                  ]
                },
                { $sqrt: '$contributions.contribution_receipt_amount' }
              ]
            },
            zeroTo50Count: {
              $cond: [
                {
                  $and: [
                    {
                      $gt: ['$contributions.contribution_receipt_amount', 0]
                    },
                    {
                      $lte: ['$contributions.contribution_receipt_amount', 50]
                    }
                  ]
                },
                1,
                0
              ]
            },
            fiftyTo200Count: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 50]
                    },
                    { $lt: ['$contributions.contribution_receipt_amount', 200] }
                  ]
                },
                1,
                0
              ]
            },
            twoHundredTo500Count: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 200]
                    },
                    { $lt: ['$contributions.contribution_receipt_amount', 500] }
                  ]
                },
                1,
                0
              ]
            },
            fiveHundredTo1000Count: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 500]
                    },
                    {
                      $lt: ['$contributions.contribution_receipt_amount', 1000]
                    }
                  ]
                },
                1,
                0
              ]
            },
            oneThouTo2000Count: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 1000]
                    },
                    {
                      $lt: ['$contributions.contribution_receipt_amount', 2000]
                    }
                  ]
                },
                1,
                0
              ]
            },
            twoThouToLimitCount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 2000]
                    },
                    {
                      $lte: ['$contributions.contribution_receipt_amount', 2700]
                    }
                  ]
                },
                1,
                0
              ]
            },
            zeroTo50Amount: {
              $cond: [
                { $lt: ['$contributions.contribution_receipt_amount', 50] },
                '$contributions.contribution_receipt_amount',
                0
              ]
            },
            fiftyTo200Amount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 50]
                    },
                    { $lt: ['$contributions.contribution_receipt_amount', 200] }
                  ]
                },
                '$contributions.contribution_receipt_amount',
                0
              ]
            },
            twoHundredTo500Amount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 200]
                    },
                    { $lt: ['$contributions.contribution_receipt_amount', 500] }
                  ]
                },
                '$contributions.contribution_receipt_amount',
                0
              ]
            },
            fiveHundredTo1000Amount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 500]
                    },
                    {
                      $lt: ['$contributions.contribution_receipt_amount', 1000]
                    }
                  ]
                },
                '$contributions.contribution_receipt_amount',
                0
              ]
            },
            oneThouTo2000Amount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 1000]
                    },
                    {
                      $lt: ['$contributions.contribution_receipt_amount', 2000]
                    }
                  ]
                },
                '$contributions.contribution_receipt_amount',
                0
              ]
            },
            twoThouToLimitAmount: {
              $cond: [
                {
                  $and: [
                    {
                      $gte: ['$contributions.contribution_receipt_amount', 2000]
                    },
                    {
                      $lte: ['$contributions.contribution_receipt_amount', 2700]
                    }
                  ]
                },
                '$contributions.contribution_receipt_amount',
                0
              ]
            }
          }
        },
        {
          $group: {
            _id: candidate.name,
            zeroTo50Count: { $sum: '$zeroTo50Count' },
            fiftyTo200Count: { $sum: '$fiftyTo200Count' },
            twoHundredTo500Count: { $sum: '$twoHundredTo500Count' },
            fiveHundredTo1000Count: { $sum: '$fiveHundredTo1000Count' },
            oneThouTo2000Count: { $sum: '$oneThouTo2000Count' },
            twoThouToLimitCount: { $sum: '$twoThouToLimitCount' },
            grandTotalCount: { $sum: '$tally' },
            zeroTo50Amount: { $sum: '$zeroTo50Amount' },
            fiftyTo200Amount: { $sum: '$fiftyTo200Amount' },
            twoHundredTo500Amount: { $sum: '$twoHundredTo500Amount' },
            fiveHundredTo1000Amount: { $sum: '$fiveHundredTo1000Amount' },
            oneThouTo2000Amount: { $sum: '$oneThouTo2000Amount' },
            twoThouToLimitAmount: { $sum: '$twoThouToLimitAmount' },
            sumOfSquareRoots: { $sum: '$sqRoot' },
            grandTotalAmount: { $sum: '$donation' }
          }
        }
      ])
      .toArray((err, results) => {
        if (err) rej(err);
        console.log(results[0]);
        res();
      });
  });
}
